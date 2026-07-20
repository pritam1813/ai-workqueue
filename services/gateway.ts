import { db } from "../database/client";
import { jobs } from "../database/schema";
import { channel, QUEUE_NAME } from "./broker";
import { JOB_EVENTS_CHANNEL } from "../shared/events";
import { ProcessRequest } from "../shared/types";
import type { JobPayload } from "../shared/types";
import { RedisClient } from "bun";

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
          console.error(error);

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
      const listener = new RedisClient(
        process.env.REDIS_URL ?? "redis://localhost:6379",
      );
      let cancelled = false;

      const stream = new ReadableStream<string>({
        async start(controller) {
          await listener.connect();

          await listener.subscribe(
            `${JOB_EVENTS_CHANNEL}:${jobId}`,
            (message) => {
              if (cancelled) return; // stream was cancelled before message arrived
              controller.enqueue(`data: ${message}\n\n`);

              // Close stream on terminal status
              let parsed: { status: string };
              try {
                parsed = JSON.parse(message);
              } catch {
                return;
              }
              if (parsed.status === "completed" || parsed.status === "failed") {
                controller.close();
              }
            },
          );

          // Send an initial ping so the client knows the connection is open
          controller.enqueue(": connected\n\n");
        },
        cancel() {
          // Client disconnected — clean up to avoid memory leaks
          cancelled = true;
          listener.close();
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
