import { eq } from "drizzle-orm";
import { db } from "../database/client";
import { jobs } from "../database/schema";
import { channel, queue } from "./message_broker";
import { EventEmitter } from "events";

const jobEvents = new EventEmitter();

await channel.assertQueue(queue, {
  durable: true,
  arguments: {
    "x-queue-type": "quorum",
  },
});

console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", queue);
channel.consume(
  queue,
  async function (msg) {
    if (msg) {
      const content = msg.content.toString();

      // send to AI for processing and update status to processing
      await db
        .update(jobs)
        .set({ status: "processing" })
        .where(eq(jobs.id, parseInt(content)));

      jobEvents.emit(content, "processing");

      // simulate proccessing time for now
      setTimeout(() => {}, 5000);

      // After getting result update status
      await db
        .update(jobs)
        .set({ status: "completed" })
        .where(eq(jobs.id, parseInt(content)));

      jobEvents.emit(content, "completed");

      console.log(" [x] Received %s", content);
    }
  },
  {
    noAck: true,
  },
);
