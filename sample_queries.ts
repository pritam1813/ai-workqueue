// import { sql } from "bun";

import { db } from "./database/client";
import { users } from "./database/schema";

// const rows = await sql`
//   CREATE SCHEMA public;
// `;

// console.log(rows);

const r = await db.select().from(users);
console.log(r);
