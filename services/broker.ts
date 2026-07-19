import amqp from "amqplib";

const RABBITMQ_URL = process.env.RABBITMQ_URL ?? "amqp://localhost";

const connection = await amqp.connect(RABBITMQ_URL);

export const channel = await connection.createChannel();

export const QUEUE_NAME = "ai-work" as const;
