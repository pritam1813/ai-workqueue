import { drizzle } from "drizzle-orm/bun-sql";
import { SQL } from "bun";

const client = new SQL();

// Bun driver does not support schema. So no db.query usage.
export const db = drizzle({ client });
