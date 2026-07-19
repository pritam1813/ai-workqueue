import z from "zod";

/**
 * Shape of the HTTP request body from the client.
 * The client must first upload the file to storage (S3, R2, etc.)
 * and provide the resulting URL here.
 */
export const ProcessRequest = z.object({
  userId: z.number().int().positive(),
  inputUrl: z.string().url(),
});

/**
 * Shape of the message published to RabbitMQ.
 * Contains everything the processor needs — no DB read required in the worker.
 */
export type JobPayload = {
  jobId: number;
  userId: number;
  inputUrl: string;
};

/**
 * Shape of messages sent over BroadcastChannel between threads.
 * Gateway listens for these and pushes them to the client SSE stream.
 */
export type JobEvent = {
  jobId: number;
  status: "processing" | "completed" | "failed";
  result?: string;
  error?: string;
};
