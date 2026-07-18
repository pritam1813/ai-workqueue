const server = Bun.serve({
  port: 3000,
  routes: {
    "/api/process": {
      POST: async (req) => {
        const body = await req.json();
        // Accepts file or text
        console.log(body);

        //Saves pending job record in DB
        return Response.json({ jobId: "1234" });
      },
    },
  },
});

console.log(`Listening on ${server.url}`);
