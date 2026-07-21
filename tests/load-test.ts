/**
 * load-test.ts
 * ------------
 * Fires 10 concurrent HTTP POST requests to the gateway, then subscribes to
 * the SSE endpoint for each job to track when it completes.
 *
 * Reports two key metrics:
 *  - Gateway response time: how long until all 10 clients got "202 Accepted"
 *  - Total processing time: from first HTTP request to last job completion
 *
 * This simulates real client load — unlike queue-rate-limit.ts which bypasses
 * the gateway, this tests the full end-to-end path.
 *
 * Requires gateway AND processor running:
 *   bun dev:gateway    (terminal 1)
 *   bun dev:processor  (terminal 2)
 *
 * Run: bun tests/load-test.ts
 */

const GATEWAY_URL = "http://localhost:3000";
const TOTAL_REQUESTS = 10;
const TEST_USER_ID = 1;

// file:// URI so the processor reads the local sample file
const SAMPLE_FILE_URL = new URL("./sample.txt", import.meta.url).href;

// ─── Main ───────────────────────────────────────────────────────────────────

console.log(`\n${"─".repeat(60)}`);
console.log(`  🔥  Load Test`);
console.log(`  ${TOTAL_REQUESTS} concurrent requests → Gateway → Queue → Processor`);
console.log(`${"─".repeat(60)}\n`);

// ─── Step 1: Fire all requests concurrently ──────────────────────────────────

const fireStart = Date.now();
console.log(`  Firing ${TOTAL_REQUESTS} concurrent POST /api/process requests...\n`);

const jobIds = await Promise.all(
  Array.from({ length: TOTAL_REQUESTS }, async (_, i) => {
    const res = await fetch(`${GATEWAY_URL}/api/process`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ userId: TEST_USER_ID, inputUrl: SAMPLE_FILE_URL }),
    });

    const body = (await res.json()) as { jobId?: number; error?: string };

    if (!body.jobId) {
      throw new Error(`Request ${i + 1} rejected: ${body.error ?? res.status}`);
    }

    console.log(`  ⚡ Request ${i + 1} → 202 Accepted  (jobId: ${body.jobId})`);
    return body.jobId;
  }),
);

const gatewayMs = Date.now() - fireStart;
console.log(
  `\n  All ${TOTAL_REQUESTS} requests acknowledged in ${gatewayMs}ms`,
);
console.log(
  `  Job IDs: [${jobIds.join(", ")}]\n`,
);
console.log(`  Waiting for processor to complete all jobs via SSE...\n`);

// ─── Step 2: Subscribe to SSE for each job ───────────────────────────────────

const sseStart = Date.now();
const results = new Map<number, { status: string; elapsedMs: number }>();

await Promise.all(
  jobIds.map(async (jobId) => {
    const response = await fetch(`${GATEWAY_URL}/jobs/${jobId}/events`);
    if (!response.body) throw new Error(`No SSE stream for job ${jobId}`);

    const reader = response.body.getReader();
    const decoder = new TextDecoder();

    try {
      outer: while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        // A single chunk can contain multiple SSE lines — split and process each
        const chunk = decoder.decode(value, { stream: true });
        for (const line of chunk.split("\n")) {
          if (!line.startsWith("data: ")) continue;

          let data: { status: string };
          try {
            data = JSON.parse(line.slice(6)) as { status: string };
          } catch {
            continue;
          }

          if (data.status === "processing") {
            console.log(`  ⚙️  Job ${jobId} processing...`);
            continue;
          }

          if (data.status === "completed" || data.status === "failed") {
            const elapsedMs = Date.now() - sseStart;
            results.set(jobId, { status: data.status, elapsedMs });

            const icon = data.status === "completed" ? "✅" : "❌";
            console.log(
              `  ${icon} Job ${jobId} ${data.status} at +${(elapsedMs / 1000).toFixed(2)}s`,
            );
            break outer;
          }
        }
      }
    } finally {
      reader.cancel();
    }
  }),
);

const totalMs = Date.now() - fireStart;
const succeeded = [...results.values()].filter((r) => r.status === "completed").length;
const failed = TOTAL_REQUESTS - succeeded;

console.log(`\n${"─".repeat(60)}`);
console.log(`  📊  Results`);
console.log(`${"─".repeat(60)}`);
console.log(
  `  Gateway response time  : ${gatewayMs}ms`,
);
console.log(`  (all ${TOTAL_REQUESTS} clients got "202 Accepted" immediately)`);
console.log(`  Total processing time  : ${(totalMs / 1000).toFixed(2)}s`);
console.log(`  (first request → last job completion)`);
console.log(`  Average / job          : ${(totalMs / TOTAL_REQUESTS / 1000).toFixed(2)}s`);
console.log(`  Completed              : ${succeeded} / ${TOTAL_REQUESTS}`);
console.log(`  Failed                 : ${failed}`);
console.log(`${"─".repeat(60)}\n`);

process.exit(0);
