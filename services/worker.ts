import { EventEmitter } from "events";
const jobEvents = new EventEmitter();

Bun.serve({
  port: 3001,
  routes: {
    "/jobs/:id/events": (req, server) => {
      server.timeout(req, 0);
      const jobId = req.params.id;

      // Declare listener here so both start() and cancel() close over the same reference.
      let listener: (status: string) => void;

      const stream = new ReadableStream({
        start(controller) {
          listener = (status: string) => {
            controller.enqueue(`data: ${JSON.stringify({ status })}\n\n`);
          };
          jobEvents.on(jobId, listener);
        },
        cancel() {
          // Called automatically when the client disconnects.
          jobEvents.off(jobId, listener);
        },
      });

      return new Response(stream, {
        headers: {
          "Content-Type": "text/event-stream",
          "Cache-Control": "no-cache",
        },
      });
    },

    // Trigger endpoint for manual testing — remove when RabbitMQ is wired up.
    "/jobs/:id/trigger": {
      POST: async (req) => {
        const jobId = req.params.id;
        const { status } = await req.json();
        jobEvents.emit(jobId, status);
        return Response.json({ ok: true });
      },
    },
  },
});
