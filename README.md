# AI Work Queue

> An asynchronous job processing system that decouples long-running Gemini AI inference from synchronous HTTP — so clients never wait, and no job is ever lost.

---

## The Problem

When a user submits a document for AI analysis, the Gemini API can take 5–30 seconds to respond. A naïve synchronous server would hold the HTTP connection open for the entire duration:

```
Client ──── POST /process ──────────────────────── (30 seconds later) ──── 200 OK
                           ↑ connection is blocked, client is waiting
```

Under concurrent traffic this breaks down immediately — and if the server crashes mid-request, the job is simply lost.

## The Solution

Decouple request handling from heavy processing using a message queue:

```
Client ──── POST /process ──── 202 Accepted (jobId)   ← returns in ~50ms
                ↓ (async, in background)
          RabbitMQ Queue → Processor → Gemini API
                ↓ (when done)
          Client receives result via SSE stream
```

---

## Architecture

![System Architecture](./docs/architecture.jpg)

```mermaid
flowchart LR
    Client(["🌐 Client"])

    subgraph Gateway ["API Gateway (Bun :3000)"]
        GW["POST /api/process\nGET /jobs/:id/events"]
    end

    subgraph Queue ["Message Broker"]
        RMQ[("🐇 RabbitMQ\nQuorum Queue\npersistent + durable")]
    end

    subgraph Worker ["Background Processor"]
        PROC["Worker\nprefetch=1\n5 RPM rate limiter"]
    end

    subgraph Storage ["Persistence"]
        PG[("🐘 PostgreSQL\nDrizzle ORM")]
        RDS[("⚡ Redis\nPub/Sub")]
    end

    EXT["✨ Gemini AI API"]

    Client -- "POST /api/process" --> GW
    GW -. "202 Accepted (jobId)" .-> Client
    GW -- "INSERT job (pending)" --> PG
    GW -- "publish payload" --> RMQ
    RMQ -- "consume (prefetch=1)" --> PROC
    PROC -- "UPDATE status" --> PG
    PROC -- "PUBLISH event" --> RDS
    RDS -- "SUBSCRIBE" --> GW
    GW -. "SSE: processing → completed" .-> Client
    PROC -- "generateContent()" --> EXT
```

---

## How It Works

### 1. Request Intake (Gateway)
The client sends a `POST /api/process` with a `userId` and `inputUrl`. The gateway:
- Validates the request with **Zod**
- Inserts a job record with `status: "pending"` into **PostgreSQL**
- Publishes the job payload to **RabbitMQ** (persistent — survives broker restarts)
- Immediately returns `202 Accepted` with the `jobId`

### 2. Queue & Backpressure (RabbitMQ)
RabbitMQ holds all queued jobs durably on disk. The processor uses `prefetch(1)` — a **backpressure signal** that tells the broker "don't deliver the next message until I ACK the current one." This prevents the processor from being flooded regardless of how many jobs are in the queue.

### 3. Processing (Processor Worker)
The processor:
- Updates the job to `status: "processing"` in the database
- Publishes a `processing` event to Redis
- Calls the **Gemini AI API** with a proactive rate limiter (12 second minimum gap between calls — enforcing the 5 RPM free tier limit)
- Updates the job to `status: "completed"` with the AI result
- **ACKs the message** — only after all steps succeed. If the process crashes before ACK, RabbitMQ re-delivers the job automatically.

### 4. Real-time Updates (Redis Pub/Sub + SSE)
The gateway subscribes to a per-job Redis channel (`job-events:{jobId}`) for each active SSE connection. When the processor publishes a status event, the gateway pushes it directly to the client's SSE stream. The stream closes automatically on terminal states (`completed` / `failed`).

---

## Key Design Decisions

### Why RabbitMQ over an in-memory queue?
An in-process queue (simple array + worker loop) achieves the same throughput **in the happy path**. The difference shows up on failure:

| Scenario | In-memory queue | RabbitMQ |
|---|---|---|
| Process crash with 10 jobs queued | All 10 lost forever | All 10 redelivered on restart |
| Job fails mid-processing | Result lost | Re-queued for retry via DLQ |
| Scale to multiple workers | Requires custom coordination | Just run more consumers |

### Why `prefetch(1)` instead of higher concurrency?
With the Gemini free tier capped at **5 requests per minute**, running 3 parallel jobs would exhaust the quota in 4 calls. `prefetch(1)` + a 12-second proactive throttle ensures exactly 5 calls per minute regardless of queue depth — no 429 errors.

### Why Redis Pub/Sub for SSE instead of polling?
The gateway and processor run as separate Bun worker threads (or processes). They share no memory. Redis acts as the inter-process message bus — the processor publishes one event, and every gateway subscriber (one per active SSE connection) receives it instantly. No database polling loop required.

### Why SSE over WebSockets?
Jobs are one-directional: the client only needs to *receive* status updates, never send them. SSE is simpler, HTTP-native, and automatically reconnects. WebSockets would add unnecessary bidirectional complexity.

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Runtime | [Bun](https://bun.sh) | HTTP server, worker threads, TypeScript execution |
| Message Queue | [RabbitMQ](https://rabbitmq.com) (Quorum Queue) | Durable, fault-tolerant job delivery |
| Cache / Pub-Sub | [Redis](https://redis.io) | Real-time inter-process event streaming |
| Database | [PostgreSQL](https://postgresql.org) + [Drizzle ORM](https://orm.drizzle.team) | Job metadata, user records, result persistence |
| AI | [Google Gemini API](https://ai.google.dev) (`@google/genai`) | AI inference |
| Validation | [Zod](https://zod.dev) | Runtime request schema validation |
| Language | TypeScript | End-to-end type safety |

---

## Project Structure

```
ai-workqueue/
├── index.ts                  # Entry point — spawns gateway + processor as Bun Workers
├── services/
│   ├── gateway.ts            # HTTP server: /api/process + /jobs/:id/events (SSE)
│   ├── processor.ts          # RabbitMQ consumer + Gemini caller + rate limiter
│   └── broker.ts             # Shared RabbitMQ channel
├── database/
│   ├── client.ts             # Drizzle + Bun SQL client
│   └── schema.ts             # users, jobs tables + relations
├── shared/
│   ├── types.ts              # Zod schemas + TypeScript types
│   └── events.ts             # Shared Redis channel name constant
├── tests/
│   ├── sample.txt            # Local text file used in test runs
│   ├── direct-rate-limit.ts  # Baseline: calls Gemini directly (no queue)
│   ├── queue-rate-limit.ts   # Queue test: publishes to RabbitMQ, tracks via Redis
│   └── load-test.ts          # HTTP load test: 10 concurrent requests via SSE
├── docs/
│   └── architecture.jpg      # Architecture diagram
└── drizzle/                  # Generated migration files
```

---

## Getting Started

### Prerequisites

- [Bun](https://bun.sh) v1.1+
- [Docker](https://docker.com) (for RabbitMQ + Redis + PostgreSQL)
- A [Google Gemini API key](https://ai.google.dev)

### 1. Start Infrastructure

```bash
# PostgreSQL
docker run -d --name postgres \
  -e POSTGRES_PASSWORD=ai-work \
  -p 5432:5432 postgres

# RabbitMQ (with management UI at localhost:15672)
docker run -d --name rabbitmq \
  -p 5672:5672 -p 15672:15672 \
  rabbitmq:3-management

# Redis
docker run -d --name redis \
  -p 6379:6379 redis
```

### 2. Configure Environment

Create a `.env` file in the project root:

```env
DB_USER=postgres
DB_PASSWORD=ai-work
DB_HOST=localhost
DB_PORT=5432
DB_NAME=postgres
DATABASE_URL=postgres://postgres:ai-work@localhost:5432/postgres
REDIS_URL=redis://localhost:6379
RABBITMQ_URL=amqp://localhost
GEMINI_API_KEY=your-api-key-here
```

### 3. Install Dependencies & Migrate

```bash
bun install
bun db:push
```

### 4. Seed a Test User

```sql
-- Run in any PostgreSQL client
INSERT INTO users (name, email) VALUES ('Test User', 'test@example.com');
-- Note the returned ID (likely 1)
```

---

## Running

```bash
# Run gateway + processor together (as Bun Worker threads)
bun dev

# Or run separately — useful for seeing logs independently
bun dev:gateway    # Terminal 1
bun dev:processor  # Terminal 2
```

### Submit a Job

```bash
curl -X POST http://localhost:3000/api/process \
  -H "Content-Type: application/json" \
  -d '{"userId": 1, "inputUrl": "https://raw.githubusercontent.com/w3c/web-performance/master/README.md"}'

# Response:
# {"jobId": 1}
```

### Stream Job Status

```bash
curl -N http://localhost:3000/jobs/1/events

# → data: {"status":"processing"}
# → data: {"status":"completed","result":"The Web Performance Working Group..."}
```

---

## Test Scripts

All three tests process the same workload (10 jobs summarising `tests/sample.txt`) so results are directly comparable.

```bash
# Baseline: direct Gemini calls, no queue
bun test:direct

# Queue pipeline: publish to RabbitMQ, track completions via Redis
# Requires: bun dev:processor running
bun test:queue

# Full HTTP load test: 10 concurrent requests + SSE completion tracking
# Requires: bun dev:gateway + bun dev:processor running
bun test:load
```

### Benchmark Results (free tier, 5 RPM)

| Test | Client wait for acknowledgement | Total time (10 jobs) | Crash-safe? |
|---|---|---|---|
| Direct (no queue) | Waits the full ~109s | ~109s | ❌ |
| Queue-based | **169ms** (all 10 enqueued) | ~109s | ✅ |
| HTTP Load Test | **66ms** (all 10 got 202) | ~109s | ✅ |

The total processing time is the same — the bottleneck is the Gemini API quota, not the queue. The queue's value is **instant client acknowledgement** and **zero job loss on failure**, not raw throughput.

---

## Planned Features

- [ ] Frontend (Vite + React) — file upload, drag-and-drop preview, live SSE status
- [ ] Authentication — AWS Cognito or Clerk
- [ ] Serverless migration — AWS Lambda for the gateway
- [ ] Dead Letter Queue — automated retry with exponential backoff for failed jobs
- [ ] Billing table — per-user job usage tracking

---

## License

MIT
