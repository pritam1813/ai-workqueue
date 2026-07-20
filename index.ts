/**
 * Entry point — spawns gateway and processor as separate Bun Worker threads.
 *
 * Both workers communicate job status via Redis Pub/Sub (not BroadcastChannel),
 * so they could also run as fully separate processes if needed.
 * - Gateway:   publishes jobs to RabbitMQ, subscribes to Redis for SSE pushes
 * - Processor: consumes from RabbitMQ, publishes status updates to Redis
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
