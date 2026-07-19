import { db } from "../database/client";
import { jobs } from "../database/schema";
import { channel, QUEUE_NAME } from "./broker";
import { JOB_EVENTS_CHANNEL } from "../shared/events";
import { ProcessRequest } from "../shared/types";
import type { JobPayload, JobEvent } from "../shared/types";

// Each Worker thread gets its own BroadcastChannel instance.
// Bun connects all instances sharing the same name within the same process.
const broadcast = new BroadcastChannel(JOB_EVENTS_CHANNEL);

// In-memory map of jobId → SSE stream controller for active connections.
// When a job event arrives via BroadcastChannel, we look up the controller
// and push the update directly — no polling needed.
const sseClients = new Map<number, ReadableStreamDefaultController<string>>();

// Listen for job events from the processor thread and push to SSE streams
broadcast.onmessage = (event: MessageEvent<JobEvent>) => {
  const { jobId, status, result, error } = event.data;
  const controller = sseClients.get(jobId);

  if (!controller) return; // client may have disconnected

  controller.enqueue(
    `data: ${JSON.stringify({ status, result, error })}\n\n`,
  );

  // Close the SSE stream once the job reaches a terminal state
  if (status === "completed" || status === "failed") {
    controller.close();
    sseClients.delete(jobId);
  }
};

const server = Bun.serve({
  port: 3000,
  routes: {
    "/api/process": {
      POST: async (req) => {
        try {
          const body = ProcessRequest.parse(await req.json());

          // Insert job with pending status (default) and the file URL
          const [job] = await db
            .insert(jobs)
            .values({
              userId: body.userId,
              input_url: body.inputUrl,
            })
            .returning();

          if (!job) {
            return Response.json(
              { error: "Failed to create job" },
              { status: 500 },
            );
          }

          // Publish full payload to RabbitMQ — processor needs no DB read
          const payload: JobPayload = {
            jobId: job.id,
            userId: job.userId,
            inputUrl: body.inputUrl,
          };

          channel.sendToQueue(
            QUEUE_NAME,
            Buffer.from(JSON.stringify(payload)),
            {
              // Message survives RabbitMQ restart
              persistent: true,
            },
          );

          return Response.json({ jobId: job.id }, { status: 202 });
        } catch (error) {
          return Response.json(
            { error: "Invalid request body" },
            { status: 400 },
          );
        }
      },
    },

    "/jobs/:id/events": (req, server) => {
      // Disable request timeout — SSE connections are long-lived
      server.timeout(req, 0);

      const jobId = parseInt(req.params.id);

      if (isNaN(jobId)) {
        return new Response("Invalid job ID", { status: 400 });
      }

      const stream = new ReadableStream<string>({
        start(controller) {
          sseClients.set(jobId, controller);

          // Send an initial ping so the client knows the connection is open
          controller.enqueue(`: connected\n\n`);
        },
        cancel() {
          // Client disconnected — clean up to avoid memory leaks
          sseClients.delete(jobId);
        },
      });

      return new Response(stream, {
        headers: {
          "Content-Type": "text/event-stream",
          "Cache-Control": "no-cache",
          Connection: "keep-alive",
        },
      });
    },
  },
});

console.log(`[gateway] Listening on ${server.url}`);
