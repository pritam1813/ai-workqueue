import z from "zod";

export const ProcessRequest = z.object({
  userId: z.number().int().positive(),
});
