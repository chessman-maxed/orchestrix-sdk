# Orchestrix

[![npm package](https://img.shields.io/npm/v/orchestrix-sdk.svg)](https://www.npmjs.com/package/orchestrix-sdk)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Orchestrix** is an open-source, high-performance distributed job processing system built on Node.js, Express, Redis/BullMQ, and PostgreSQL. It enables web applications and microservices to offload heavy background tasks asynchronously with durable tracking, active cancellation, priority scheduling, and DLQ management.

It includes a official Node.js / TypeScript SDK ([`orchestrix-sdk`](https://www.npmjs.com/package/orchestrix-sdk)) for job submission and status monitoring.

---

## 1. Project Overview

Orchestrix processes background jobs through a decoupled architecture:
- **REST API Server**: Built with Express, exposing endpoints for job creation, bulk enqueueing, cancellation, retries, and status queries.
- **Redis & BullMQ**: In-memory message broker providing fast job queues, concurrency controls, delay mechanisms, and priority scheduling.
- **Background Workers**: Autonomous worker processes that pull jobs from Redis queues, execute task logic, and manage state transitions.
- **PostgreSQL Database**: Persistent storage tracking long-term job history, execution status, worker metrics, batches, dependencies, and schedules.

---

## 2. Key Capabilities

Only capabilities fully implemented in the repository are listed below:

- **Asynchronous Job Submission**: Enqueue background tasks with custom JSON payloads.
- **Job Priority & Delay**: Configure job priorities (1–10) and execution delays (milliseconds).
- **Idempotency Deduplication**: Request deduplication using `Idempotency-Key` headers backed by Redis locks with a 24-hour TTL.
- **Active Job Cancellation**: Cancel pending, delayed, or actively running jobs with Redis Pub/Sub coordination and abort signals.
- **Dead Letter Queue (DLQ) & Manual Replay**: Automatic routing of exhausted jobs to a DLQ, with support for manual job replay while preserving history.
- **Batch Processing**: Submit and track bulk jobs (`/jobs/bulk`) with progress aggregation.
- **Job Dependencies**: Define DAG execution dependencies (`/jobs/:id/dependencies`).
- **Cron Scheduling**: Schedule recurring jobs using cron syntax (`/schedules`).
- **Durable History & Metrics**: Historical tracking and aggregation of worker, queue, and workflow metrics in PostgreSQL.

---

## 3. Architecture

```
                               ┌─────────────────┐
                               │  Client App     │
                               │ (or SDK Client) │
                               └────────┬────────┘
                                        │ HTTP REST
                                        ▼
                               ┌─────────────────┐
                               │ Orchestrix API  │
                               │  (Express Server)│
                               └───┬─────────┬───┘
                                   │         │
                 Enqueue / Read    │         │ Sync History
                                   ▼         ▼
                           ┌───────────┐ ┌──────────────┐
                           │   Redis   │ │  PostgreSQL  │
                           │ (BullMQ)  │ │ (Job History)│
                           └─────┬─────┘ └──────────────┘
                                 │
                                 │ Pull & Process Jobs
                                 ▼
                           ┌───────────┐
                           │  Worker   │
                           │ Processes │
                           └───────────┘
```

1. **Client**: Sends job creation requests via HTTP REST API or the `orchestrix-sdk`.
2. **Orchestrix API**: Validates incoming requests and enqueues jobs into **Redis/BullMQ**. Simultaneously writes durable job records into **PostgreSQL**.
3. **Redis/BullMQ**: Acts as the high-throughput task queue managing job states (`waiting`, `active`, `completed`, `failed`, `delayed`).
4. **Workers**: Independent worker processes poll Redis for available work, execute tasks, report progress, and update status.
5. **PostgreSQL**: Serves as the persistent audit log for durable job execution histories, batch tracking, and system metrics.

---

## 4. Technology Stack

- **Runtime**: Node.js (>= 18.0.0) & TypeScript
- **API Framework**: Express.js
- **Queue Engine**: BullMQ & ioredis (Redis 6+)
- **Database**: PostgreSQL (`pg`)
- **SDK Packaging**: CommonJS & TypeScript declaration files (`.d.ts`)

---

## 5. SDK Quick Start

The official Node.js SDK allows applications to enqueue and inspect Orchestrix jobs programmatically.

### Installation

```bash
npm install orchestrix-sdk
```

### Usage Example (JavaScript / CommonJS)

```javascript
const { OrchestrixClient } = require('orchestrix-sdk');

// Initialize client with your running Orchestrix server base URL
const client = new OrchestrixClient('http://localhost:3000');

async function main() {
  try {
    // Create a new background job
    const job = await client.jobs.create(
      {
        task: 'send-email',
        recipient: 'user@example.com',
        templateId: 'welcome-email',
      },
      {
        priority: 2, // 1 (lowest) to 10 (highest)
        delay: 5000, // Delay execution by 5000ms
        idempotencyKey: 'msg_987654321', // Optional request deduplication
      }
    );

    console.log('Job created successfully:');
    console.log(`- Job ID: ${job.jobId}`);
    console.log(`- Status Message: ${job.message}`);
    console.log(`- Is Duplicate: ${job.isDuplicate}`);
  } catch (error) {
    console.error('Error creating job:', error.message);
  }
}

main();
```

### Usage Example (TypeScript / ES Modules)

```typescript
import { OrchestrixClient, CreateJobResponse } from 'orchestrix-sdk';

const client = new OrchestrixClient({
  baseUrl: 'http://localhost:3000',
  timeoutMs: 10000,
});

async function main(): Promise<void> {
  const response: CreateJobResponse = await client.jobs.create({
    reportType: 'monthly-summary',
    userId: 42,
  });

  console.log(`Enqueued job ID: ${response.jobId}`);
}

main();
```

---

## 6. Retrieving Job Status

To check the execution status, progress, or return value of a submitted job, pass its `jobId` to `client.jobs.get()`:

```javascript
const { OrchestrixClient } = require('orchestrix-sdk');

const client = new OrchestrixClient('http://localhost:3000');

async function checkStatus(jobId) {
  try {
    const job = await client.jobs.get(jobId);

    console.log(`Job ID: ${job.id}`);
    console.log(`State: ${job.state}`); // 'waiting', 'active', 'completed', 'failed', 'delayed', etc.
    console.log(`Progress: ${job.progress}%`);
    console.log(`Attempts: ${job.attemptsMade} / ${job.maxAttempts}`);

    if (job.state === 'completed') {
      console.log('Result:', job.returnvalue);
    } else if (job.state === 'failed') {
      console.log('Failure Reason:', job.failedReason);
    }
  } catch (error) {
    console.error('Failed to retrieve job details:', error.message);
  }
}

checkStatus('1');
```

---

## 7. Prerequisites and Configuration

> **Important**: The `orchestrix-sdk` package is a client library. It connects to an active Orchestrix backend over HTTP. It does **not** launch or embed the Orchestrix API server, Redis, or PostgreSQL automatically.

### Requirements for SDK Usage

1. A running Orchestrix API server instance (e.g., at `http://localhost:3000` or a remote server URL).
2. Network access from the SDK application to the Orchestrix API server.

---

## 8. SDK Limitations

The current `orchestrix-sdk` (v0.1.0) is focused on core job management:
- **Supported Methods**:
  - `client.jobs.create(data, options)` — Enqueuing single jobs with priority, delay, and idempotency headers.
  - `client.jobs.get(jobId, options)` — Inspecting job status, execution state, progress, and results.

- **Unexposed Backend Capabilities**:
  While the Orchestrix REST API server supports bulk job creation (`POST /jobs/bulk`), active job cancellation (`POST /jobs/:id/cancel`), manual retries (`POST /jobs/:id/retry`), schedule management (`/schedules`), batch metrics, and DLQ listings, these capabilities are currently accessible via HTTP REST endpoints and are planned for future SDK releases.

---

## 9. Links

- **npm Package**: [https://www.npmjs.com/package/orchestrix-sdk](https://www.npmjs.com/package/orchestrix-sdk)
- **GitHub Repository**: [https://github.com/chessman-maxed/Orchestrix](https://github.com/chessman-maxed/Orchestrix)

---

## 10. Local Development

### System Prerequisites
- Node.js >= 18.0.0
- Redis instance running on `localhost:6379`
- PostgreSQL instance running on `localhost:5432`

### Backend Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/chessman-maxed/Orchestrix.git
   cd Orchestrix
   ```

2. **Install dependencies**:
   ```bash
   npm install
   ```

3. **Configure environment variables**:
   Create a `.env` file or supply environment variables:
   ```env
   PORT=3000
   REDIS_HOST=localhost
   REDIS_PORT=6379
   POSTGRES_HOST=localhost
   POSTGRES_PORT=5432
   POSTGRES_DB=orchestrix
   POSTGRES_USER=postgres
   POSTGRES_PASSWORD=postgres
   ```

4. **Build the TypeScript backend**:
   ```bash
   npm run build
   ```

5. **Start the API Server**:
   ```bash
   npm run start:api
   ```

6. **Start a Worker Process**:
   ```bash
   npm run start:worker
   ```

### SDK Development & Local Integration Testing

To build and test the SDK locally within this repository:

1. **Build the SDK**:
   ```bash
   cd orchestrix-sdk
   npm install
   npm run build
   ```

2. **Run Integration Test** (requires running API server at `http://localhost:3000`):
   ```bash
   npm run test:integration
   ```

---

## License

This project is licensed under the [MIT License](LICENSE).
