import { db } from "../database/client";
import { jobs } from "../database/schema";
import { ProcessRequest } from "../shared/types";

const server = Bun.serve({
  port: 3000,
  routes: {
    "/api/process": {
      POST: async (req) => {
        try {
          const body = ProcessRequest.parse(await req.json());

          //Saves pending job record in DB
          const [job] = await db
            .insert(jobs)
            .values({
              userId: body.userId,
            })
            .returning();

          if (!job) {
            return Response.json(
              { error: "Failed to create job" },
              { status: 500 },
            );
          }

          return Response.json({ jobId: job.id });
        } catch (error) {
          return Response.json(
            { error: "Invalid JSON format" },
            { status: 400 },
          );
        }
      },
    },
  },
});

console.log(`Listening on ${server.url}`);
