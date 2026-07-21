/**
 * direct-rate-limit.ts
 * --------------------
 * Baseline test: calls Gemini directly — no RabbitMQ, no HTTP gateway, no queue.
 * Applies the same 5 RPM proactive throttle and retry logic as the processor.
 *
 * Purpose: measure how long 10 sequential Gemini calls take when you own the
 * rate limiting yourself, without any queue overhead.
 *
 * Run: bun tests/direct-rate-limit.ts
 */
import { GoogleGenAI } from "@google/genai";

const RPM_LIMIT = 5;
const MIN_INTERVAL_MS = (60 / RPM_LIMIT) * 1000; // 12,000ms between calls
const TOTAL_JOBS = 10;

const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY! });

// Read the local sample file once — all jobs summarize the same text
const sampleText = await Bun.file(
  new URL("./sample.txt", import.meta.url),
).text();

let lastCallTime = 0;

// ─── Helpers ────────────────────────────────────────────────────────────────

function parseRetryDelay(error: unknown): number | null {
  if (!(error instanceof Error)) return null;
  const msg = error.message;
  if (!msg.includes("429") && !msg.includes("503")) return null;
  const match = msg.match(/retry in (\d+(?:\.\d+)?)s/i);
  return match ? Math.ceil(parseFloat(match[1]!)) * 1000 : 60_000;
}

async function callGemini(index: number, attempt = 1): Promise<string> {
  // Proactive throttle: ensure MIN_INTERVAL_MS gap between Gemini calls
  const elapsed = Date.now() - lastCallTime;
  const wait = MIN_INTERVAL_MS - elapsed;
  if (wait > 0) {
    process.stdout.write(
      `  [${index}] throttling ${(wait / 1000).toFixed(1)}s... `,
    );
    await Bun.sleep(wait);
    process.stdout.write(`go\n`);
  }
  lastCallTime = Date.now();

  try {
    const result = await ai.models.generateContent({
      model: "gemini-3.1-flash-lite",
      contents: `Summarize the following text in one sentence:\n\n${sampleText.substring(0, 2000)}`,
    });
    return result.text?.trim() ?? "No result";
  } catch (err) {
    const delay = parseRetryDelay(err);
    if (delay !== null && attempt <= 3) {
      console.warn(
        `  [${index}] rate limited — retrying in ${delay / 1000}s (attempt ${attempt}/3)`,
      );
      lastCallTime = 0; // reset so throttle doesn't stack on top of retry delay
      await Bun.sleep(delay);
      return callGemini(index, attempt + 1);
    }
    throw err;
  }
}

// ─── Main ───────────────────────────────────────────────────────────────────

console.log(`\n${"─".repeat(60)}`);
console.log(`  🔬  Direct Rate Limit Test`);
console.log(`  ${TOTAL_JOBS} jobs  |  ${RPM_LIMIT} RPM  |  No queue, no HTTP`);
console.log(`${"─".repeat(60)}\n`);

const totalStart = Date.now();
const timings: number[] = [];

for (let i = 1; i <= TOTAL_JOBS; i++) {
  const jobStart = Date.now();
  process.stdout.write(`  [${i}/${TOTAL_JOBS}] calling Gemini... `);

  const result = await callGemini(i);
  const ms = Date.now() - jobStart;
  timings.push(ms);

  console.log(`✅ ${(ms / 1000).toFixed(2)}s`);
  console.log(
    `         → "${result.slice(0, 90)}${result.length > 90 ? "…" : ""}"`,
  );
}

const totalMs = Date.now() - totalStart;

console.log(`\n${"─".repeat(60)}`);
console.log(`  📊  Results`);
console.log(`${"─".repeat(60)}`);
console.log(`  Total time     : ${(totalMs / 1000).toFixed(2)}s`);
console.log(`  Average / job  : ${(totalMs / TOTAL_JOBS / 1000).toFixed(2)}s`);
console.log(`  Fastest job    : ${(Math.min(...timings) / 1000).toFixed(2)}s`);
console.log(`  Slowest job    : ${(Math.max(...timings) / 1000).toFixed(2)}s`);
console.log(`${"─".repeat(60)}\n`);
