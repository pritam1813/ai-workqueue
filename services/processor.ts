import { eq } from "drizzle-orm";
import { db } from "../database/client";
import { jobs } from "../database/schema";
import { channel, QUEUE_NAME } from "./broker";
import { JOB_EVENTS_CHANNEL } from "../shared/events";
import type { JobPayload, JobEvent } from "../shared/types";

// Each Worker thread gets its own BroadcastChannel instance.
// Bun connects all instances sharing the same name within the same process.
const broadcast = new BroadcastChannel(JOB_EVENTS_CHANNEL);

// Assert queue on startup — processor owns this declaration.
await channel.assertQueue(QUEUE_NAME, {
  durable: true,
  arguments: {
    "x-queue-type": "quorum",
  },
});

// Limit to 3 concurrent jobs — this is your Gemini API rate limiter.
// RabbitMQ will not deliver a 4th message until one of the 3 is ACK'd.
await channel.prefetch(3);

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

      // 2. Notify gateway thread → client SSE stream
      broadcast.postMessage({
        jobId,
        status: "processing",
      } satisfies JobEvent);

      // 3. Call AI model with the file URL
      const result = await processWithAI(inputUrl);

      // 4. Persist result and mark completed
      await db
        .update(jobs)
        .set({ status: "completed", result })
        .where(eq(jobs.id, jobId));

      // 5. Notify gateway thread → client SSE stream
      broadcast.postMessage({
        jobId,
        status: "completed",
        result,
      } satisfies JobEvent);

      // 6. ACK only after everything succeeded — RabbitMQ won't re-deliver
      channel.ack(msg);

      console.log(`[processor] Job ${jobId} completed`);
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : "Unknown error";

      console.error(`[processor] Job ${jobId} failed:`, errorMessage);

      // Update DB to failed
      await db
        .update(jobs)
        .set({ status: "failed" })
        .where(eq(jobs.id, jobId));

      // Notify gateway thread → client SSE stream
      broadcast.postMessage({
        jobId,
        status: "failed",
        error: errorMessage,
      } satisfies JobEvent);

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

/**
 * Placeholder for Gemini API integration.
 * Replace with actual @google/generative-ai call.
 */
async function processWithAI(inputUrl: string): Promise<string> {
  // TODO: replace with actual Gemini API call
  // const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY!);
  // const model = genAI.getGenerativeModel({ model: "gemini-1.5-pro" });
  // const result = await model.generateContent([{ fileData: { fileUri: inputUrl } }]);
  // return result.response.text();

  await Bun.sleep(5000); // simulate network + AI processing time
  return `Analysis complete for: ${inputUrl}`;
}
