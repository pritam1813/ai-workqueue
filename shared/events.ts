/**
 * Shared BroadcastChannel name for cross-thread job status events.
 * Both gateway and processor threads create their own BroadcastChannel
 * instance using this name — Bun connects them automatically within
 * the same process.
 */
export const JOB_EVENTS_CHANNEL = "job-events" as const;
