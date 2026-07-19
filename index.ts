/**
 * Entry point — spawns gateway and processor as separate Bun Worker threads.
 *
 * Running both in the same process means:
 * - BroadcastChannel connects them without any external infrastructure (no Redis)
 * - Bun's module cache is NOT shared between workers (each thread is isolated)
 * - Each worker gets its own AMQP connection (gateway = publisher, processor = consumer)
 * - If processor crashes, gateway stays alive — users can still submit jobs
 */

const gatewayWorker = new Worker(
  new URL("./services/gateway.ts", import.meta.url),
);

const processorWorker = new Worker(
  new URL("./services/processor.ts", import.meta.url),
);

// Surface worker errors to the main thread logs
gatewayWorker.addEventListener("error", (err) => {
  console.error("[main] Gateway worker error:", err.message);
});

processorWorker.addEventListener("error", (err) => {
  console.error("[main] Processor worker error:", err.message);
});

console.log("[main] Gateway and Processor workers started");
