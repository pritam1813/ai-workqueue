import amqp from "amqplib";

const connection = await amqp.connect("amqp://localhost");

export const channel = await connection.createChannel();

export const queue = "ai-work";

await channel.assertQueue(queue, {
  durable: true,
  arguments: {
    "x-queue-type": "quorum",
  },
});
