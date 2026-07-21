/**
 * queue-rate-limit.ts
 * --------------------
 * Publishes 10 jobs directly to RabbitMQ (skipping the HTTP gateway),
 * then subscribes to Redis to track when all jobs complete.
 *
 * This measures the same 10-job workload as direct-rate-limit.ts but through
 * the full queue pipeline, so you can compare the overhead of the queue system.
 *
 * Requires the processor to be running first:
 *   bun dev:processor
 *
 * Run: bun tests/queue-rate-limit.ts
 */
import amqp from "amqplib";
import { RedisClient } from "bun";
import { drizzle } from "drizzle-orm/bun-sql";
import { SQL } from "bun";
import { jobs } from "../database/schema";
import type { JobPayload } from "../shared/types";
import { JOB_EVENTS_CHANNEL } from "../shared/events";

const TOTAL_JOBS = 10;
const QUEUE_NAME = "ai-work";
const TEST_USER_ID = 1;

// file:// URI so the processor's getTextContent() reads it from disk
const SAMPLE_FILE_URL = new URL("./sample.txt", import.meta.url).href;

// ─── Setup ──────────────────────────────────────────────────────────────────

const db = drizzle({ client: new SQL() });

const connection = await amqp.connect(process.env.RABBITMQ_URL ?? "amqp://localhost");
const channel = await connection.createChannel();

// Subscriber connection (in subscribe mode — cannot be used for anything else)
const subscriber = new RedisClient(process.env.REDIS_URL ?? "redis://localhost:6379");
await subscriber.connect();

// ─── Main ───────────────────────────────────────────────────────────────────

console.log(`\n${"─".repeat(60)}`);
console.log(`  🐇  Queue Rate Limit Test`);
console.log(`  ${TOTAL_JOBS} jobs  |  5 RPM  |  RabbitMQ → Processor → Redis`);
console.log(`${"─".repeat(60)}\n`);

const totalStart = Date.now();

// 1. Insert all 10 jobs into DB and publish to RabbitMQ immediately.
//    The processor will throttle itself — we just flood the queue.
const jobIds: number[] = [];

for (let i = 0; i < TOTAL_JOBS; i++) {
  const [job] = await db
    .insert(jobs)
    .values({ userId: TEST_USER_ID, input_url: SAMPLE_FILE_URL })
    .returning();

  if (!job) throw new Error(`Failed to insert job ${i + 1}`);

  const payload: JobPayload = {
    jobId: job.id,
    userId: TEST_USER_ID,
    inputUrl: SAMPLE_FILE_URL,
  };

  channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(payload)), {
    persistent: true,
  });

  jobIds.push(job.id);
  console.log(`  📤 Job ${job.id} enqueued`);
}

const enqueueMs = Date.now() - totalStart;
console.log(`\n  All ${TOTAL_JOBS} jobs enqueued in ${enqueueMs}ms`);
console.log(`  Waiting for processor to complete all jobs...\n`);

// 2. Subscribe to Redis for all job channels and collect completion events.
//    Redis in subscribe mode allows multiple SUBSCRIBE calls on one connection.
const completions = new Map<number, { status: string; elapsedMs: number }>();

await new Promise<void>((resolve) => {
  for (const jobId of jobIds) {
    subscriber.subscribe(
      `${JOB_EVENTS_CHANNEL}:${jobId}`,
      (message: string) => {
        let event: { status: string };
        try {
          event = JSON.parse(message) as { status: string };
        } catch {
          return;
        }

        if (event.status === "processing") {
          console.log(`  ⚙️  Job ${jobId} processing...`);
          return;
        }

        if (event.status === "completed" || event.status === "failed") {
          const elapsedMs = Date.now() - totalStart;
          completions.set(jobId, { status: event.status, elapsedMs });

          const icon = event.status === "completed" ? "✅" : "❌";
          console.log(
            `  ${icon} Job ${jobId} ${event.status} at +${(elapsedMs / 1000).toFixed(2)}s`,
          );

          if (completions.size === TOTAL_JOBS) resolve();
        }
      },
    );
  }
});

const totalMs = Date.now() - totalStart;
const succeeded = [...completions.values()].filter((c) => c.status === "completed").length;
const failed = TOTAL_JOBS - succeeded;

console.log(`\n${"─".repeat(60)}`);
console.log(`  📊  Results`);
console.log(`${"─".repeat(60)}`);
console.log(`  Total time     : ${(totalMs / 1000).toFixed(2)}s`);
console.log(`  Enqueue time   : ${enqueueMs}ms (all ${TOTAL_JOBS} jobs accepted instantly)`);
console.log(`  Average / job  : ${(totalMs / TOTAL_JOBS / 1000).toFixed(2)}s`);
console.log(`  Completed      : ${succeeded} / ${TOTAL_JOBS}`);
console.log(`  Failed         : ${failed}`);
console.log(`${"─".repeat(60)}\n`);

await subscriber.close();
await connection.close();
process.exit(0);
