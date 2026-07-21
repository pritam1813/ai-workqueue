import { eq } from "drizzle-orm";
import { db } from "../database/client";
import { jobs } from "../database/schema";
import { channel, QUEUE_NAME } from "./broker";
import { JOB_EVENTS_CHANNEL } from "../shared/events";
import type { JobPayload } from "../shared/types";
import { RedisClient } from "bun";
import { GoogleGenAI } from "@google/genai";

// Redis publisher — used to notify the gateway of job status changes.
// A dedicated connection is required for publishing (cannot share with subscriber).
const writer = new RedisClient(
  process.env.REDIS_URL ?? "redis://localhost:6379",
);
await writer.connect();

// Assert queue on startup — processor owns this declaration.
await channel.assertQueue(QUEUE_NAME, {
  durable: true,
  arguments: {
    "x-queue-type": "quorum",
  },
});

// prefetch(1): Only hold 1 unacknowledged message at a time.
// This is the backpressure signal — RabbitMQ will not deliver the next job
// until the current one is ACK'd. Combined with the rate limiter in
// processWithAI(), this guarantees we never exceed 5 requests/min.
await channel.prefetch(1);

console.log("[processor] Ready — waiting for jobs in queue:", QUEUE_NAME);

channel.consume(
  QUEUE_NAME,
  async (msg) => {
    if (!msg) return;

    let payload: JobPayload;
    try {
      payload = JSON.parse(msg.content.toString()) as JobPayload;
    } catch {
      // Malformed message — discard immediately, don't requeue
      channel.nack(msg, false, false);
      return;
    }

    const { jobId, inputUrl } = payload;

    try {
      // 1. Mark as processing in DB (for crash-recovery observability)
      await db
        .update(jobs)
        .set({ status: "processing" })
        .where(eq(jobs.id, jobId));

      // 2. Publish status to Redis — gateway subscriber will push to SSE stream
      await writer.publish(
        `${JOB_EVENTS_CHANNEL}:${jobId}`,
        JSON.stringify({ status: "processing" }),
      );

      // 3. Call AI model with the file URL
      const result = await processWithAI(inputUrl);

      // 4. Persist result and mark completed
      await db
        .update(jobs)
        .set({ status: "completed", result })
        .where(eq(jobs.id, jobId));

      // 5. Publish status + result to Redis
      await writer.publish(
        `${JOB_EVENTS_CHANNEL}:${jobId}`,
        JSON.stringify({ status: "completed", result }),
      );

      // 6. ACK only after everything succeeded — RabbitMQ won't re-deliver
      channel.ack(msg);

      console.log(`[processor] Job ${jobId} completed`);
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : "Unknown error";

      console.error(`[processor] Job ${jobId} failed:`, errorMessage);

      // Update DB to failed
      await db.update(jobs).set({ status: "failed" }).where(eq(jobs.id, jobId));

      // Publish failure status to Redis
      await writer.publish(
        `${JOB_EVENTS_CHANNEL}:${jobId}`,
        JSON.stringify({ status: "failed", error: errorMessage }),
      );

      // NACK without requeue — let the Dead Letter Queue handle retries
      channel.nack(msg, false, false);
    }
  },
  {
    // Manual ACK — message is only removed from queue after explicit ack()
    // If the process crashes mid-job, RabbitMQ will re-deliver the message
    noAck: false,
  },
);

// ---------------------------------------------------------------------------
// Rate Limiter
// ---------------------------------------------------------------------------
// Free tier: 5 requests per minute → 1 request every 12 seconds minimum.
// We track the timestamp of the last Gemini call and sleep the difference
// before every new call. This is proactive throttling — we never exceed the
// quota in the first place rather than reacting to 429s after the fact.
// ---------------------------------------------------------------------------
const RPM_LIMIT = 5;
const MIN_INTERVAL_MS = (60 / RPM_LIMIT) * 1000; // 12,000ms
let lastCallTime = 0;

const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });

/**
 * Parses the retry delay (in ms) from a Gemini 429 error message.
 * The API returns strings like "Please retry in 15.56s".
 * Returns null if the error is not a 429 or delay cannot be parsed.
 */
function parseRetryDelay(error: unknown): number | null {
  if (!(error instanceof Error)) return null;

  const message = error.message;

  // Only handle rate limit and overload errors
  if (!message.includes("429") && !message.includes("503")) return null;

  const match = message.match(/retry in (\d+(?:\.\d+)?)s/i);
  if (match) return Math.ceil(parseFloat(match[1]!)) * 1000;

  // Fallback if retry delay isn't in the message
  return 60_000;
}

/**
 * Calls the Gemini API with proactive rate limiting and retry-on-429 logic.
 *
 * Flow:
 *  1. Sleep until 12s have elapsed since the last call (proactive throttle)
 *  2. Call Gemini
 *  3. If 429/503, sleep the API-provided retryDelay and try again (max 3 attempts)
 */
/**
 * Reads text content from either a remote HTTP URL or a local file:// URI.
 * Test scripts pass file:// URIs (e.g. new URL("./sample.txt", import.meta.url).href)
 * so no external HTTP server is needed for local testing.
 */
async function getTextContent(inputUrl: string): Promise<string> {
  if (inputUrl.startsWith("file://")) {
    return await Bun.file(new URL(inputUrl)).text();
  }
  const response = await fetch(inputUrl);
  if (!response.ok) {
    throw new Error(
      `Failed to fetch input URL: ${response.status} ${response.statusText}`,
    );
  }
  return await response.text();
}

async function processWithAI(inputUrl: string, attempt = 1): Promise<string> {
  // --- Proactive throttle ---
  const elapsed = Date.now() - lastCallTime;
  const waitTime = MIN_INTERVAL_MS - elapsed;

  if (waitTime > 0) {
    console.log(
      `[processor] Rate limiter: waiting ${(waitTime / 1000).toFixed(1)}s before next Gemini call`,
    );
    await Bun.sleep(waitTime);
  }

  // Record the call time before the request so parallel paths
  // (if prefetch is ever raised) also see the correct timestamp.
  lastCallTime = Date.now();

  try {
    console.log(`[processor] Reading content from: ${inputUrl}`);
    const textData = await getTextContent(inputUrl);

    console.log(`[processor] Calling Gemini (attempt ${attempt}/3)...`);
    const result = await ai.models.generateContent({
      model: "gemini-3.1-flash-lite",
      contents: `Please summarize the following text in one sentence:\n\n${textData.substring(0, 2000)}`,
    });

    return result.text || "No result generated.";
  } catch (error) {
    // --- Reactive retry (safety net for unexpected 429/503) ---
    const retryDelay = parseRetryDelay(error);

    if (retryDelay !== null && attempt <= 3) {
      console.warn(
        `[processor] Got rate-limit/overload error. Retrying in ${retryDelay / 1000}s (attempt ${attempt}/3)`,
      );
      // Reset lastCallTime so the proactive throttle doesn't add extra wait on top
      lastCallTime = 0;
      await Bun.sleep(retryDelay);
      return processWithAI(inputUrl, attempt + 1);
    }

    // Non-retryable error or exhausted retries — bubble up to the consumer
    throw error;
  }
}
