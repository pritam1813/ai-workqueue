ALTER TABLE "jobs" ADD COLUMN "result" text;--> statement-breakpoint
ALTER TABLE "jobs" ADD COLUMN "created_at" timestamp DEFAULT now() NOT NULL;--> statement-breakpoint
ALTER TABLE "jobs" ADD COLUMN "updated_at" timestamp DEFAULT now();