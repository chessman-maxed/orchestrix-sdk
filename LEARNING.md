# Orchestrix — Distributed Task Queue & Job Scheduler

## 1. What are we building?
**Orchestrix** is a **Distributed Task Queue & Job Scheduler**. 

In plain terms, it is a background system that allows web applications to offload time-consuming tasks (like sending emails, processing videos, generating reports, or calling external APIs) so that the user doesn't have to wait for the web application to finish the work before getting a response.

### Real-World Problem Solved
Imagine you click "Generate Monthly Financial PDF" on a website. Generating that PDF might take 10 seconds.
- **Without a Queue**: The HTTP request stays open for 10 seconds. Your browser loading spinner spins. If 1,000 users click the button at once, the API server gets overloaded, runs out of memory, and crashes.
- **With Orchestrix (Queue)**: The API server instantly accepts your request in 5 milliseconds, gives you a `jobId` (`"Job #123 created!"`), and puts the work in a queue. A separate background process (a Worker) picks up the job and works on it. You can check the status of `Job #123` anytime without freezing your browser.

---

## 2. The Big Picture

### Real-World Analogy: The Restaurant
Think of Orchestrix like a busy restaurant:
- **Client**: The Customer who orders food.
- **API Server**: The Waiter taking the order. The waiter writes the order down on a ticket and instantly moves on to serve the next customer. The waiter does *not* cook the food.
- **Queue / Redis**: The Order Board (spindle) in the kitchen where physical order tickets hang in sequence.
- **Worker**: The Line Cook in the kitchen. The cook takes an order ticket off the board, cooks the meal (takes 3 seconds), and marks the order as "Ready".

### Technical Architecture Overview
1. **Client** makes an HTTP POST request to the API Server.
2. **API Server** uses **BullMQ** to push a job payload into **Redis**.
3. **Redis** acts as the central data store storing all pending and active jobs.
4. **Worker** constantly listens to Redis, locks an available job, executes the task logic, and updates Redis when done.
5. **Client** can poll `GET /jobs/:id` at any time to inspect the state stored in Redis.

---

## 3. Core Concepts

### Backend / Server
- **What it means**: A computer or program running continuously to process requests and serve data over a network.
- **Why we need it**: To handle business logic, process data, and keep state centralized.
- **How it is used in Orchestrix**: Our Node.js Express server is the entry point receiving requests from clients.
- **Analogy**: The physical restaurant building where service happens.

### API (Application Programming Interface)
- **What it means**: A set of exposed rules and endpoints allowing different programs to talk to each other.
- **Why we need it**: To give clients a standardized way to interact with our queue system.
- **How it is used in Orchestrix**: We expose `POST /jobs` to submit work and `GET /jobs/:id` to check status.
- **Analogy**: The menu presented to the customer.

### HTTP Request / Response
- **What it means**: The standard protocol used on the web. A client sends a *Request* (e.g., POST with JSON data), and the server returns a *Response* (e.g., status 201 with JSON result).
- **Why we need it**: It is the universal language for web and mobile clients.
- **How it is used in Orchestrix**: Communication mechanism between external clients and our API server.
- **Analogy**: Ordering your meal from the waiter (Request) and the waiter giving you an order claim ticket (Response).

### Redis
- **What it means**: An extremely fast, in-memory key-value data store.
- **Why we need it**: In-memory data access takes microseconds. Redis allows our API and Workers to quickly share job state across different processes and machines.
- **How it is used in Orchestrix**: Acts as the storage engine where BullMQ stores queue lists, job data, and job statuses.
- **Analogy**: The order spindle/counter top in the kitchen where tickets sit.

### Queue
- **What it means**: A First-In, First-Out (FIFO) data structure where items are processed in the order they arrive.
- **Why we need it**: To order incoming work sequentially and prevent overload.
- **How it is used in Orchestrix**: Holds waiting jobs until a worker is ready to process them.
- **Analogy**: The line of customers waiting at a ticket counter.

### Producer
- **What it means**: Any component that creates jobs and pushes them into the queue.
- **Why we need it**: To generate tasks from incoming requests or events.
- **How it is used in Orchestrix**: Our API server acts as the producer by calling `taskQueue.add(...)`.
- **Analogy**: The waiter placing new order tickets on the kitchen board.

### Consumer / Worker
- **What it means**: An independent, long-running process that pulls jobs out of the queue and executes them.
- **Why we need it**: To perform heavy work off the main HTTP server thread.
- **How it is used in Orchestrix**: `src/worker.ts` picks up jobs from Redis and simulates 3 seconds of background work.
- **Analogy**: The cook in the kitchen taking tickets off the board and cooking meals.

### BullMQ
- **What it means**: A Node.js / TypeScript library that implements high-performance queues on top of Redis.
- **Why we need it**: Instead of writing complex raw Redis Lua scripts to manage queues, concurrency, and locks, BullMQ provides clean abstractions (`Queue`, `Worker`, `Job`).
- **How it is used in Orchestrix**: Imports `Queue` in `src/queue.ts` and `Worker` in `src/worker.ts`.
- **Analogy**: The manager system organizing kitchen tickets, ensuring cooks don't grab the same ticket twice.

### Asynchronous / Background Processing
- **What it means**: Executing tasks independently of the main program flow without blocking execution.
- **Why we need it**: Keeps API responses fast (<10ms) regardless of how long the underlying task takes.
- **How it is used in Orchestrix**: The API returns a job ID immediately while the worker processes the job in the background.
- **Analogy**: Getting a pager at a coffee shop so you can sit down while your coffee is being prepared.

### Distributed Systems
- **What it means**: A network of autonomous computing nodes (processes or servers) communicating with each other to achieve a common goal.
- **Why we need it**: Allows scaling each part of the system independently (e.g., running 1 API server and 10 Worker machines).
- **How it is used in Orchestrix**: API server, Redis, and Worker run as completely separate processes.
- **Analogy**: A chain of coffee shops sharing a centralized inventory database.

---

## 4. Our Current Architecture

```
Client
  │
  │ (1) POST /jobs  OR  (7) GET /jobs/:id
  ▼
API Server (Express) ───[ (2) queue.add() ]─────────────────► Redis (Port 6379)
  │                                                                 │
  ├───────[ (2b) Best-effort insertJob() ]──────┐                   │
  │                                             ▼                   │
  │                                    PostgreSQL (Port 5432)       │
  │                                             ▲                   │
  │                                             │ (5) Best-effort   │
  │                                             │     status updates│
  ▼                                             │                   │
Worker Process (BullMQ Worker) ─────────────────┴───────────────────┘
  │                                              (3) Pulls & locks active job
  │ (4) Simulates work (3 seconds)
  ▼
Job Result / Completion Event (Stored in Redis & PostgreSQL)
```

### Explanation of Diagram Arrows:
1. **Client → API Server**: Client sends HTTP requests to create (`POST /jobs`) or check status (`GET /jobs/:id`).
2. **API Server → BullMQ Queue & Redis**: Express handler uses BullMQ client methods to format and submit new tasks to Redis.
3. **API Server → PostgreSQL**: Express handler performs a best-effort `insertJob()` call to persist initial `pending` job metadata in PostgreSQL.
4. **Redis → Worker Process**: The Worker polls/subscribes to Redis, fetching the next available `waiting` job and locking it as `active`.
5. **Worker Process → PostgreSQL**: Worker updates PostgreSQL status to `running`, `completed`, `retrying`, `failed`, or `stalled` asynchronously across job lifecycle events.
6. **Worker Process → Job Result**: Worker executes code, finishes work after 3 seconds, and writes the `completed` state and return payload back into Redis.


---

## 5. What Happens When I Submit a Job? (Lifecycle)

```
1. Client POST /jobs {"task": "email"}
   ↓
2. Express Handler calls taskQueue.add('default-job', payload)
   ↓
3. BullMQ generates unique ID (e.g., "1") and writes job to Redis (State: 'waiting')
   ↓
4. API responds to Client: 201 Created {"jobId": "1"} (Total time: ~5ms)
   ↓
5. Worker process detects waiting job in Redis
   ↓
6. Worker locks job in Redis (State changes to 'active')
   ↓
7. Worker executes async process (Waits 3 seconds)
   ↓
8. Worker finishes and writes result back to Redis (State changes to 'completed')
   ↓
9. Client calls GET /jobs/1 → API fetches job state from Redis → Returns status 'completed'
```

---

## 6. Our Current Codebase

```
Orchestrix/
├── package.json         # Project metadata, scripts, and dependencies
├── tsconfig.json        # TypeScript compiler options
├── LEARNING.md          # Personal learning & architectural documentation
└── src/
    ├── redis.ts         # Shared Redis connection options
    ├── queue.ts         # BullMQ Queue instance (Producer)
    ├── api.ts           # Express HTTP server & API route handlers
    └── worker.ts        # Standalone Worker process (Consumer)
```

### File Details:
- **`src/redis.ts`**:
  - *Why it exists*: Centralizes connection options (`host`, `port`) so API and Worker connect to the same Redis instance.
  - *What it does*: Exports `redisConnection` object configured with `maxRetriesPerRequest: null`.
  - *Communicates with*: Used by `queue.ts` and `worker.ts`.

- **`src/queue.ts`**:
  - *Why it exists*: Initializes the producer queue instance.
  - *What it does*: Instantiates `new Queue('orchestrix-queue', ...)` using `redisConnection`.
  - *Communicates with*: Used by `api.ts` to add jobs and read job states.

- **`src/api.ts`**:
  - *Why it exists*: Handles incoming HTTP requests from users.
  - *What it does*: Implements `POST /jobs` (creates jobs) and `GET /jobs/:id` (queries job status).
  - *Communicates with*: Communicates with Clients over HTTP and `taskQueue` over BullMQ.

- **`src/worker.ts`**:
  - *Why it exists*: Dedicated background process to consume and execute work.
  - *What it does*: Listens on `orchestrix-queue`, runs simulated work (3 seconds delay), logs events.
  - *Communicates with*: Connects directly to Redis via BullMQ `Worker`.

---

## 7. Important Code Concepts

### 1. Redis Connection Configuration (`src/redis.ts`)
```typescript
export const redisConnection: ConnectionOptions = {
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: parseInt(process.env.REDIS_PORT || '6379', 10),
  maxRetriesPerRequest: null,
};
```
*Explanation*: BullMQ requires `maxRetriesPerRequest` to be set to `null` so it can manage connection retries and blocking commands internally without crashing.

### 2. Adding a Job in API (`src/api.ts`)
```typescript
app.post('/jobs', async (req: Request, res: Response) => {
  const payload = req.body;
  const job = await taskQueue.add('default-job', payload);
  res.status(201).json({ message: 'Job submitted successfully', jobId: job.id });
});
```
*Explanation*: `taskQueue.add(...)` takes a job name string and a payload object. It returns a promise containing the created `Job` metadata (including `job.id`).

### 3. Fetching Job Status (`src/api.ts`)
```typescript
app.get('/jobs/:id', async (req: Request, res: Response) => {
  const job = await taskQueue.getJob(req.params.id);
  if (!job) return res.status(404).json({ error: 'Job not found' });
  const state = await job.getState();
  res.json({ id: job.id, state, data: job.data, returnvalue: job.returnvalue });
});
```
*Explanation*: `getJob(id)` looks up the job in Redis. `job.getState()` resolves to the job's current status string (`waiting`, `active`, `completed`, `failed`).

### 4. Worker Processing Logic (`src/worker.ts`)
```typescript
const worker = new Worker(
  QUEUE_NAME,
  async (job: Job) => {
    console.log(`[Worker] Started processing Job ID: ${job.id}`);
    await delay(3000); // Simulate work
    return { result: 'Job processed successfully' };
  },
  { connection: redisConnection }
);
```
*Explanation*: The worker accepts the queue name and a processor function `async (job) => { ... }`. The return value of this processor is automatically saved in Redis as `job.returnvalue`.

---

## 8. Redis and BullMQ

It is crucial to understand the distinction between **BullMQ** and **Redis**:

| Feature | BullMQ | Redis |
|---|---|---|
| **Role** | Application Library (TypeScript/JavaScript) | Storage & Messaging Engine (C binary) |
| **Location** | Runs inside your Node.js processes (`api.ts`, `worker.ts`) | Runs as a separate database service/process |
| **Responsibility** | Knows how to package jobs, set statuses, handle retries, manage events | Stores keys/hashes/lists in RAM and executes commands |

**Key Takeaway**:
- **BullMQ is the software architect and protocol**.
- **Redis is the database storage infrastructure**.
- The Queue is an abstraction maintained *by* BullMQ *inside* Redis.

---

## 9. Workers & Concurrency Explained

### 1. What Concurrency Means
Concurrency is the ability to process multiple tasks in overlapping timeframes rather than sequentially one-by-one.

### 2. Multiple Worker Processes vs Single Worker Concurrency
- **Multiple Worker Processes**: Running separate Node.js processes (e.g. Process PID 1010 and PID 2020). Each process gets its own OS memory heap and CPU thread.
- **Worker Concurrency Setting (`WORKER_CONCURRENCY`)**: The `concurrency` option passed to BullMQ's `new Worker(queue, processor, { concurrency })`. It dictates how many jobs a *single* worker process can handle asynchronously in parallel inside its Node.js event loop.
- **Combined Effect**: If you run **2 Worker Processes**, each configured with `WORKER_CONCURRENCY=2`, your system handles **4 jobs in parallel**.

### 3. How BullMQ Coordinates Workers Through Redis
All worker processes connect to the shared Redis instance. BullMQ uses **atomic Lua scripts** inside Redis so that when a job moves from `waiting` to `active`, Redis locks the job to exactly ONE worker ID. Two workers will **never** process the exact same queued job simultaneously.

### 4. Job Distribution Example (10 Jobs, 2 Worker Processes @ Concurrency 2)
Suppose 10 jobs (3s each) are submitted in bulk:
- **Process A (PID 1010, Concurrency 2)** takes Job 1 and Job 2 immediately.
- **Process B (PID 2020, Concurrency 2)** takes Job 3 and Job 4 immediately.
- **At t=3s**: All 4 jobs finish. Process A grabs Job 5 and Job 6; Process B grabs Job 7 and Job 8.
- **At t=6s**: Process A grabs Job 9 and Job 10.
- **Total Duration**: ~9 seconds (instead of 30 seconds with 1 single-concurrent worker).

---

## 10. Failure Scenarios

Here is how our current system handles various failure scenarios:

| Scenario | What happens in current system? | Handled Automatically? |
|---|---|---|
| **API Server crashes** | Jobs already in Redis remain safe. New HTTP requests fail. Once API restarts, it can resume reading status. | ✅ Data safe in Redis |
| **Worker crashes BEFORE picking up job** | Job remains safely in `waiting` state in Redis. | ✅ Safe in Redis |
| **Worker crashes DURING job processing** | Job gets stuck in `active` state (stalled). Current code does not yet auto-retry or re-queue it. | ⚠️ Needs stall handler / retries |
| **Redis crashes** | In-memory queue data could be lost unless Redis persistence (RDB/AOF) is enabled. | ❌ Redis dependent |
| **Multiple Workers run simultaneously** | BullMQ & Redis atomic locks prevent two workers from processing the same job. | ✅ Handled automatically by BullMQ |

---

## 11. What We Have NOT Built Yet (Future Roadmap)

- [x] Multiple worker scaling & concurrency testing
- [x] Automatic retry mechanism (on job failure)
- [x] Exponential backoff strategy
- [x] Manual replay mechanism for failed jobs
- [ ] Dead-letter queue (DLQ) for dedicated secondary queue storage
- [x] Job priority levels (High / Normal / Low)
- [x] Scheduled / Delayed jobs (run job after specified delay)
- [ ] Recurring cron jobs (run every night at midnight)
- [x] Idempotency key handling (prevent duplicate submissions)
- [x] Worker heartbeat monitoring & active lock renewal
- [x] Worker stall recovery (re-queueing crashed active jobs)
- [ ] PostgreSQL persistent storage for historical analytics
- [ ] User authentication & authorization
- [ ] API Key management
- [ ] Rate limiting on job submission
- [ ] Real-time web monitoring dashboard (Bull-Board)
- [ ] System load testing and benchmarking scripts
- [ ] Dockerized multi-container setup (Docker Compose)

---

## 12. Interview Preparation: Questions You Should Be Able to Answer

### Q1: Why do we need a queue?
**Answer**: To decouple client requests from heavy task execution. It prevents API HTTP timeouts, keeps API response times fast (<10ms), and absorbs sudden traffic spikes without overloading servers.

### Q2: Why not process the job directly inside the API request?
**Answer**: Processing directly blocks the single-threaded Node.js event loop or HTTP connection. If a task takes 5 seconds and 1,000 requests hit the server, the server will run out of sockets, memory, or crash.

### Q3: Why use Redis for the queue?
**Answer**: Redis is an in-memory store that provides sub-millisecond read/write speeds and atomic operations. This makes it ideal for fast job locking and multi-process queue synchronization.

### Q4: Why use BullMQ?
**Answer**: BullMQ provides a robust TypeScript-native wrapper over Redis that handles complex queue semantics—like job locking, state management, delayed execution, and event emission—out of the box.

### Q5: What is a worker?
**Answer**: A worker is a standalone background process that continuously pulls jobs from the queue in Redis, executes the required work, and records the outcome.

### Q6: What happens if a worker crashes?
**Answer**: The job remains in Redis. In advanced setups, BullMQ detects that the worker stopped renewing its lock (stalled) and moves the job back to `waiting` or `failed` so another worker can retry it.

### Q7: What happens when 10,000 jobs are submitted at once?
**Answer**: Redis safely stores all 10,000 jobs in the `waiting` list. The API responds quickly to all 10,000 users. Workers pick up jobs sequentially at their own pace without crashing the system.

### Q8: How do we scale workers?
**Answer**: By launching additional instances of the worker process across multiple terminal windows, CPU cores, or server nodes. They all connect to the same Redis instance and automatically divide the workload.

### Q9: What happens if two workers try to process the same job?
**Answer**: Redis uses atomic locking primitives (Lua scripts). Whichever worker's lock command reaches Redis first gets the job; the second worker gets nothing and moves to the next job.

### Q10: What is asynchronous processing?
**Answer**: A execution model where a task is triggered without waiting for it to finish before moving on to the next instruction.

### Q11: What makes this system distributed?
**Answer**: The API server, the Redis data store, and the Worker process are completely decoupled. They can run on different physical machines across the network and scale independently.

---

## 13. Mental Model ("Remember This")

- **API receives the request.**
- **API does not perform the heavy work.**
- **Queue holds work that needs to be done.**
- **Redis stores the queue data in memory.**
- **Worker performs the work asynchronously.**
- **Job ID allows checking status anytime.**
- **Multiple workers allow parallel processing.**
- **Decoupling provides reliability and scalability.**
- **Retry = another attempt for the same job.**
- **Replay = a new job based on an old failed job.**
- **Priority = scheduling importance among waiting jobs.**
- **Priority controls which WAITING job gets picked next; it does not interrupt a running job.**
- **Delay controls WHEN a job becomes available.**
- **Idempotency controls WHETHER a new job gets created upon API request.**
- **Retry controls WHAT happens when execution fails.**
- **Worker recovery controls WHAT happens when a worker process crashes mid-job.**
- **Replay creates a NEW job lifecycle.**

---

## 14. Current Project Status

### What Currently Works:
- ✅ Redis server connection established (`127.0.0.1:6379`).
- ✅ Express API server running on port `3000`.
- ✅ Single job submission via `POST /jobs`.
- ✅ Bulk job submission via `POST /jobs/bulk` (`{ "count": 10 }`).
- ✅ Worker process identification via Process PID logs.
- ✅ Environment-driven worker concurrency via `WORKER_CONCURRENCY`.
- ✅ Parallel execution across multiple worker processes.
- ✅ Real-time status inspection via `GET /jobs/:id`.
- ✅ Automatic retry mechanism with exponential backoff on transient failures.
- ✅ Manual replay mechanism (`POST /jobs/:id/retry`) creating new linked jobs.
- ✅ Job priority levels (1–10) with non-preemptive worker scheduling.
- ✅ Delayed jobs support (`POST /jobs` with optional `delay` in milliseconds).
- ✅ Idempotency key handling (HTTP header `Idempotency-Key` with Redis `SET NX`).
- ✅ Worker failure & stalled job recovery (`lockDuration`, `stalledInterval`, `maxStalledCount`).

---

## 15. How to Run & Verify Multiple Workers (Windows Friendly)

### 1. Start Redis Container

**First-time creation:**
```powershell
docker run --name orchestrix-redis -p 6379:6379 redis:alpine
```

**Subsequent starts:**
```powershell
docker start orchestrix-redis
```

### 2. Start the API Server
```powershell
npm run start:api
```

### 3. Start Worker Process #1 (Concurrency = 2)
In a new terminal window:
```powershell
$env:WORKER_CONCURRENCY=2; npm run start:worker
```
*(Output: `⚡ Starting Worker Process [PID: 1234] with Concurrency: 2`)*

### 4. Start Worker Process #2 (Concurrency = 2)
In another terminal window:
```powershell
$env:WORKER_CONCURRENCY=2; npm run start:worker
```
*(Output: `⚡ Starting Worker Process [PID: 5678] with Concurrency: 2`)*

### 5. Submit 10 Bulk Jobs
In another terminal window:
```powershell
curl -X POST http://localhost:3000/jobs/bulk -H "Content-Type: application/json" -d "{\"count\": 10}"
```

### 6. Verify Concurrent Execution in Logs
Observe both worker terminal windows:
- You will see Worker `[PID: 1234]` processing 2 jobs simultaneously while Worker `[PID: 5678]` processes 2 jobs simultaneously.
- 4 jobs will start at the exact same millisecond timestamp!

---

## 16. Concurrency Benchmarking Methodology & Results

### Objective
To measure the real-world throughput difference when processing a batch of 10 jobs (3 seconds per job) between:
- **Experiment A**: 1 Worker Process with `WORKER_CONCURRENCY=1` (Sequential execution).
- **Experiment B**: 2 Worker Processes with `WORKER_CONCURRENCY=2` (Parallel execution, capacity = 4 jobs).

### Why Old Jobs Must Be Cleared First
If old jobs are left in Redis from previous test runs, new workers might pick up pending/stalled jobs before our benchmark batch is submitted, skewing measurement start and end timestamps. Running `docker restart orchestrix-redis` or `redis-cli flushall` flushes in-memory queue state prior to each test.

### Benchmarking Formula
$$\text{Theoretical Execution Time} = \left\lceil \frac{\text{Total Jobs}}{\text{Total Concurrency Capacity}} \right\rceil \times \text{Job Duration (3s)}$$

- **Experiment A Capacity**: $1 \text{ worker} \times 1 \text{ concurrency} = 1 \text{ slot}$.  
  $$\text{Theoretical Time} = \left\lceil \frac{10}{1} \right\rceil \times 3\text{s} = 30\text{ seconds}$$
- **Experiment B Capacity**: $2 \text{ workers} \times 2 \text{ concurrency} = 4 \text{ slots}$.  
  $$\text{Theoretical Time} = \left\lceil \frac{10}{4} \right\rceil \times 3\text{s} = 3 \text{ batches} \times 3\text{s} = 9\text{ seconds}$$

### Measured Results Placeholder (Fill in after running live test)

| Metric | Experiment A (1 Worker, Conc=1) | Experiment B (2 Workers, Conc=2) |
|---|---|---|
| **Total Worker Capacity** | 1 job slot | 4 job slots |
| **Batch Submission Timestamp** | `[Pending User Run]` | `[Pending User Run]` |
| **Last Job Completed Timestamp**| `[Pending User Run]` | `[Pending User Run]` |
| **Measured Total Elapsed Time**| `__ seconds` | `__ seconds` |
| **Speedup Ratio** | 1.0x (Baseline) | `__x faster` |

### Why the Processing Times Differ
In **Experiment A**, Redis feeds 1 job at a time to the single worker thread. The 10th job must wait for the first 9 jobs to finish sequentially ($9 \times 3\text{s} = 27\text{s}$ wait time).

In **Experiment B**, 4 jobs start simultaneously at $t=0\text{s}$. At $t=3\text{s}$, the next 4 jobs start ($t=3\text{s}$ to $6\text{s}$). At $t=6\text{s}$, the final 2 jobs start ($t=6\text{s}$ to $9\text{s}$).

---

## 17. Failure Handling, Retries & Manual Replay *(Implemented)*

### 1. What are Job Retries?
A retry mechanism automatically re-executes a failed job up to a maximum number of configured attempts before declaring it permanently failed.

### 2. Why Transient Failures Need Retries
Transient failures are temporary glitches (such as brief network drops, rate limits, or 503 HTTP errors) that resolve themselves within seconds. Retrying automatically handles these glitches without losing data or requiring manual developer intervention.

### 3. Why Immediate Retries Can Be Harmful
Retrying instantly (e.g. 10 times in 1 second) can bombard a recovering server or database, causing cascading outages.

### 4. Exponential Backoff Explained
Instead of retrying immediately, **exponential backoff** delays each retry exponentially:
$$\text{Delay} = \text{Initial Delay (2000ms)} \times 2^{(\text{attempt} - 1)}$$

- **Attempt 1 Fails**: Waits $2^0 \times 2\text{s} = \mathbf{2\text{ seconds}}$ before Attempt 2.
- **Attempt 2 Fails**: Waits $2^1 \times 2\text{s} = \mathbf{4\text{ seconds}}$ before Attempt 3.
- **Attempt 3 Fails**: All 3 attempts exhausted $\rightarrow$ Status changes to `FAILED`.

---

### 5. Queue Retry Configuration
In Orchestrix, `taskQueue` is configured with default job options (`defaultJobOptions` in `src/queue.ts`):
- **`attempts: 3`**: Every job is allowed up to 3 execution attempts by default.
- **`backoff`**: Exponential backoff with an initial delay of `2000ms` (2 seconds).

Because any new job created via `taskQueue.add()` inherits these default queue options, replayed jobs automatically receive a fresh set of retry rules (3 attempts, 2s/4s backoff).

---

### 6. Automatic Retry vs. Manual Replay

#### Automatic Retry
- **Same BullMQ Job**: Operates on the existing job object inside Redis.
- **Counter Increments**: `attemptsMade` increases with each attempt ($1 \rightarrow 2 \rightarrow 3$).
- **Lifecycle Example**:
  ```text
  Attempt 1 → Attempt 2 → Attempt 3 → FAILED
  ```

#### Manual Replay
- **Completely NEW BullMQ Job**: Creates a brand-new job in Redis instead of modifying the failed one.
- **Fresh Counter**: New job starts at `Attempt 1` with `attemptsMade = 0`.
- **Original Preserved**: The original failed job remains untouched in Redis for auditing.
- **Lifecycle Example**:
  ```text
  Job 2 → FAILED after 3 attempts
       ↓ (manual replay: POST /jobs/2/retry)
  Job 4 → Attempt 1 → Attempt 2 → Attempt 3 → FAILED
  ```

---

### 7. Why the Naive Implementation Was Wrong

In our first manual retry attempt, we reused the failed BullMQ job by directly retrying it (e.g., calling BullMQ's native `job.retry()`).

**The Problem**:
When the original 3 attempts were exhausted, calling retry on the same job caused logs showing:
```text
Attempt 4 of 3
```

**Why it happened**:
The existing job's attempt history (`attemptsMade = 3`) was **not reset**. BullMQ continued incrementing the old job's counter (`3 + 1 = 4`), violating the configured limit of 3 max attempts and failing to provide a proper new 3-attempt backoff lifecycle.

---

### 8. Correct Architecture for Manual Replay

Orchestrix treats manual replay as a **NEW job execution lifecycle** rather than resetting or modifying the old failed job.

When a user calls `POST /jobs/:id/retry`:
1. **Reads Original Job**: Fetches the failed job from Redis.
2. **Creates New Job**: Calls `taskQueue.add()` with the original payload.
3. **Fresh Counter**: The new job gets `attemptsMade = 0` and starts at `Attempt 1`.
4. **Inherits Queue Defaults**: Receives default `attempts = 3` and exponential backoff ($2\text{s}, 4\text{s}$).
5. **Links to Parent Job**: Embeds audit metadata inside the new job's payload:
   - `_replayedFromJobId`: Stores original job ID (e.g., `"2"`).
   - `_previousFailedReason`: Stores original failure error message.

---

### 9. Historical Preservation

Why do we keep the original failed job in Redis instead of deleting or overwriting it?

Keeping the original failed job preserves:
- **Original Job ID**: Maintains traceability.
- **Original Attempt Count**: Keeps exact historical record of how many times it was attempted originally.
- **Failure Reason**: Retains the exact error message and stack trace.
- **Execution History**: Provides full timeline visibility for debugging.
- **Job Relationship**: Establishes a clear parent-child linkage between the original failed job and replayed job (`_replayedFromJobId`).

---

### 10. Live Testing & Verification Results

We verified this architecture with an actual live test on Orchestrix:

1. **Job 2 Failure Cycle**:
   - Submitted a job configured to fail (`POST /jobs` with `{"task": "simulate-failure"}`).
   - **Attempt 1**: Failed $\rightarrow$ delayed 2s.
   - **Attempt 2**: Failed $\rightarrow$ delayed 4s.
   - **Attempt 3**: Failed $\rightarrow$ permanently moved to `FAILED` status.

2. **Triggered Manual Replay**:
   - Called `POST /jobs/2/retry`.
   - API created a new job: **Job 4**.

3. **Job 4 Execution & Verification**:
   - **Job 4 Linked to Job 2**: Payload contained `_replayedFromJobId: "2"`.
   - **Started at Attempt 1**: `attemptsMade` reset to fresh attempt counter (`Attempt 1 of 3`).
   - **Independent Retry Cycle**: Job 4 completed its own full 3-attempt retry cycle (`Attempt 1` $\rightarrow$ `Attempt 2` $\rightarrow$ `Attempt 3` $\rightarrow$ `FAILED`).
   - **Historical Preservation**: Job 2 remained preserved in Redis in its original `FAILED` state.

---

### 11. Important Mental Model

> **Retry** = another attempt for the same job.  
> **Replay** = a new job based on an old failed job.

---

## 18. Job Priority & Scheduling Architecture *(Implemented)*

### 1. Priority Concept & Rules
Orchestrix supports per-job priority scheduling using BullMQ options:
- **Priority 1**: Highest priority (urgent tasks).
- **Priority 5**: Normal / Default priority.
- **Priority 10**: Lower priority (non-urgent background work).
- **Ordering Rule**: **Lower number = higher priority**.
- **Valid API Range**: Integer between `1` and `10`.
- **Fallback Rule**: Missing, invalid, non-integer, or out-of-range priority defaults to `5`.

#### Non-Preemptive Execution Rule
Priority affects **only the selection order of WAITING jobs** in Redis. Priority does **NOT** preempt or interrupt a job that is already `active`/running.

**Example**:
1. Worker starts processing **Job A** (Priority 10).
2. While Job A is running, **Job B** (Priority 1) arrives in the queue.
3. **Job A continues running to completion** without interruption.
4. As soon as a worker slot becomes available, **Job B** is selected before any lower-priority waiting jobs.

---

### 2. Technical Architecture & Payload vs. Job Option

```
Client ──► POST /jobs {"priority": 1} ──► Express API (parsePriority)
                                                 │
                                                 ▼
                                taskQueue.add(name, data, { priority })
                                                 │
                                                 ▼
                                Redis Queue (ZSET ordered by Priority ASC)
                                                 │
                                                 ▼
                                Worker pulls highest-priority WAITING job next
```

#### Crucial Distinction: `job.opts.priority` vs. `job.data.priority`
- **`job.data.priority`**: Merely JSON payload data stored inside the job body. BullMQ does **not** read this field for scheduling.
- **`job.opts.priority`**: BullMQ's official job option passed during queue insertion (`taskQueue.add(name, data, { priority })`). BullMQ uses this option to order waiting jobs inside Redis sorted sets.

---

### 3. Queue Defaults
In `src/queue.ts`, `taskQueue` is configured with `defaultJobOptions`:
- **`attempts: 3`**: Up to 3 attempts on failure.
- **`backoff`**: Exponential backoff with initial delay of `2000ms`.
- **`priority: 5`**: Default priority level for any job submitted without explicit priority.

---

### 4. API Endpoints Behavior
- **`POST /jobs`**: Accepts optional `priority`, validates it (1–10 integer, defaults to 5), and passes `{ priority }` as a BullMQ job option.
- **`POST /jobs/bulk`**: Accepts optional `priority` parameter and applies that priority to all jobs created in the bulk batch.
- **`GET /jobs/:id`**: Exposes `priority: job.opts.priority || 5` in response JSON for status inspection.
- **`POST /jobs/:id/retry`**: Reads `originalJob.opts.priority` and passes `{ priority: originalPriority }` when creating the replayed job, preserving original priority alongside audit metadata (`_replayedFromJobId`).

---

### 5. Worker Behavior
In `src/worker.ts`, the worker reads:
```typescript
const priority = job.opts.priority || 5;
```
It logs `(Priority: ${priority}, Attempt ${attemptNum} of ${maxAttempts})` when starting work.

---

### 6. Live Testing & Verification Results

#### Test A — Priority Storage & Extraction
Submitted a single job with `{"priority": 10}`:
- **Worker Output**: `[Worker PID] 🚀 Started Job ID: 7 (Priority: 10, Attempt 1 of 3)`.
- Verified that priority 10 was correctly stored in `job.opts.priority` and read by the worker.

#### Test B — Non-Preemptive Priority Ordering
- Configured Worker concurrency = `2`.
- Created a backlog of **Low Priority (Priority 10)** jobs.
- Submitted **Job 20 with Priority 1** while low-priority jobs were waiting.
- **Result**: Currently running active jobs completed normally. Once a worker slot freed up, **Job 20 (Priority 1) was selected immediately**, jumping ahead of all waiting Priority 10 jobs.

#### Test C — Priority Preservation Across Automatic Retries
Submitted a failing job (Job 21) with `priority: 2`:
- **Attempt 1**: `Priority 2` $\rightarrow$ Failed, delayed 2s.
- **Attempt 2**: `Priority 2` $\rightarrow$ Failed, delayed 4s.
- **Attempt 3**: `Priority 2` $\rightarrow$ Permanently `FAILED`.
- Verified BullMQ automatically retains `job.opts.priority` across all retry attempts.

#### Test D — Priority Preservation Across Manual Replay
Executed `POST /jobs/21/retry` on failed Job 21 (`priority: 2`):
- Created new **Job 22** with payload `_replayedFromJobId: "21"`.
- **Job 22 Execution**: Started with `Priority: 2` on Attempt 1.
- Verified manual replay explicitly preserves the parent job's priority.

---

### 7. Important Mental Models

> **Priority controls which WAITING job gets picked next.**  
> **It does not control or interrupt a job that is already running.**

> **Retry** = another attempt for the same job.  
> **Replay** = a new job based on an old failed job.  
> **Priority** = scheduling importance among waiting jobs.

---

### 8. Developer Implementation Note: The API Restart Gotcha

During initial testing, after updating `src/api.ts` and `src/worker.ts`, submitting a request with `{"priority": 10}` resulted in the worker logging:
```text
Priority: 5
```
even though the JSON request body contained `"priority": 10`.

**Root Cause**:
The API server process (`npm run start:api`) was still running the **old in-memory compiled code** from before the edit, so it wasn't passing `{ priority }` to `taskQueue.add()`. The worker, on the other hand, had been restarted and was correctly reading `job.opts.priority` (which fell back to queue default `5`).

**Lesson Learned**:
In Node.js backend development, long-running processes (like Express API servers) do not automatically hot-reload source code edits unless process managers (like `nodemon` or `tsx`) are used. **Always restart the API process after modifying route handler logic.**

---

## 19. Delayed Jobs Architecture *(Implemented)*

### 1. Delayed Jobs Concept

A **Delayed Job** is a task submitted to the queue that must NOT execute immediately. Instead, it remains on hold for a specified delay period before becoming available to workers.

#### Normal Job Flow vs. Delayed Job Flow

```
Normal Job Flow:
POST /jobs  ──►  Queue (waiting)  ──►  Worker  ──►  Runs immediately

Delayed Job Flow:
POST /jobs  ──►  Queue (delayed)  ──►  ⏳ Wait for Delay  ──►  Queue (waiting)  ──►  Worker  ──►  Runs
```

#### Units of Delay
Delay is specified in **milliseconds**:
- `delay: 10000` = 10 seconds
- `delay: 60000` = 60 seconds (1 minute)

---

### 2. API Behavior

`POST /jobs` accepts an optional `delay` integer parameter (in milliseconds).

#### Processing Rules:
- **Missing or Omitted Delay**: Job executes immediately (delay defaults to `0` / `undefined`).
- **Valid Positive Delay**: Job enters `delayed` state for the specified duration.
- **Invalid, Negative, or Non-Numeric Delay**: Gracefully falls back to `undefined` (immediate execution).

#### Status Inspection (`GET /jobs/:id`)
`GET /jobs/:id` exposes complete delayed job details:
- `state`: `"delayed"` (or `"waiting"`, `"active"`, `"completed"`, `"failed"`)
- `delay`: Original delay in milliseconds (e.g. `30000`)
- `attemptsMade`: Number of execution attempts made so far
- `priority`: Assigned job priority level (1–10)
- `data`: Job payload JSON
- `timestamp`: Creation timestamp

---

### 3. BullMQ & Redis Integration

Delay is passed as a BullMQ job option during queue insertion in `src/api.ts`:

```typescript
const delay = parseDelay(payload?.delay);

const job = await taskQueue.add('default-job', payload, {
  priority,
  delay,
});
```

#### How BullMQ & Redis Handle Delayed Jobs:
1. BullMQ places delayed jobs into a specialized Redis Sorted Set (`zset`) scored by target execution timestamp (`now + delay`).
2. Workers do **not** pull or execute the job while it is in the `delayed` set.
3. BullMQ's internal timer monitors the sorted set and automatically shifts the job into the `waiting` list once `currentTime >= targetTimestamp`.

---

### 4. The Delayed Job State

While the delay timer is actively running, BullMQ reports:

```json
{
  "id": "26",
  "state": "delayed",
  "delay": 60000,
  "attemptsMade": 0,
  "priority": 5
}
```

#### Key Observation:
`attemptsMade` remains **`0`** while the job is in the `delayed` state because the worker has not attempted execution yet.

---

### 5. Interaction With Priority

Delayed jobs retain their assigned `priority` value alongside their `delay` timer.

#### How Delay and Priority Interact:
- **Delay** determines **WHEN** a job becomes available for execution.
- **Priority** determines **WHICH** ready job a worker selects next once the delay expires.

#### Example Scenario:
- **Job A**: `delay = 10000` (10s), `priority = 10` (Low)
- **Job B**: `delay = 10000` (10s), `priority = 1` (High)

Both jobs sit in the `delayed` set for 10 seconds. At $t=10\text{s}$, both delay timers expire and move to the `waiting` queue simultaneously. As soon as a worker slot frees up, **Job B (Priority 1) is selected first** over Job A (Priority 10).

> **Note**: Priority controls selection order among waiting jobs; it does **not** interrupt an already running active job.

---

### 6. Interaction With Retries & Exponential Backoff

Once a delayed job's timer elapses and execution begins, the existing retry system operates normally.

#### Execution Lifecycle:
1. Initial delay timer elapses ($t = \text{delay}$).
2. Worker picks up job $\rightarrow$ `Attempt 1`.
3. If Attempt 1 fails, BullMQ triggers the configured retry rules:
   - `maxAttempts = 3`
   - Exponential backoff starting at `2000ms` (Attempt 2 after 2s, Attempt 3 after 4s).
4. Initial `delay` option only affects when the job *first* becomes ready to run; retry backoff intervals take over for subsequent execution failures.

---

### 7. Manual Replay Behavior for Failed Delayed Jobs

When a delayed job fails all attempts and an operator manually triggers `POST /jobs/:id/retry`, **the original delay is explicitly removed**.

#### Example Walkthrough:

##### Original Job (Job 27):
```json
{
  "jobId": "27",
  "delay": 10000,
  "priority": 2
}
```
- Waits 10 seconds in `delayed` state.
- Executes $\rightarrow$ fails Attempt 1 $\rightarrow$ fails Attempt 2 $\rightarrow$ fails Attempt 3 $\rightarrow$ Status `FAILED`.

##### Manual Replay (`POST /jobs/27/retry`):
```json
{
  "jobId": "28",
  "replayedFromJobId": "27",
  "priority": 2
}
```
- **Job 28 Created**: Inherits `priority = 2` and payload `_replayedFromJobId = "27"`.
- **Delay Reset**: `delay` option is set to `0` (omitted).
- **Execution**: Immediately becomes available in `waiting` state for the worker.

#### Why Replay Resets Delay:
Manual replay represents an explicit operator intervention: *"Try running this failed task again right now."* Forcing the operator to wait through another 30-second or 10-minute delay timer would defeat the purpose of an on-demand manual retry.

> **Payload Note**: The replayed job's data body (`job.data`) may still contain the original request field `"delay": 10000`, but the actual BullMQ option passed to the queue runner is `delay: undefined` / `0`.

---

### 8. Live Testing & Verification Results

All 4 delayed job test cases were executed and verified against Orchestrix:

#### Test A — Delayed Execution Success
- **Job ID**: `24`
- **Request**: `POST /jobs` with `{"task": "send-reminder", "delay": 15000}`
- **Behavior**: Job waited in Redis for 15 seconds before worker execution began.
- **Total Elapsed Time**: ~18 seconds (~15s delay + ~3s worker execution).
- **Status**: Successfully completed with state `"completed"`.

#### Test B — Delayed State Inspection
- **Job ID**: `26`
- **Request**: `POST /jobs` with `{"delay": 60000}`
- **Status Query**: Immediately called `GET /jobs/26`.
- **Verified Response**:
  - `state: "delayed"`
  - `delay: 60000`
  - `attemptsMade: 0`

#### Test C — Delayed Failing Job Retry Cycle
- **Job ID**: `27`
- **Request**: `POST /jobs` with `{"task": "simulate-failure", "delay": 10000, "priority": 2}`
- **Behavior**: Waited 10 seconds in `delayed` state $\rightarrow$ worker picked up $\rightarrow$ failed Attempt 1 $\rightarrow$ backoff 2s $\rightarrow$ failed Attempt 2 $\rightarrow$ backoff 4s $\rightarrow$ failed Attempt 3 $\rightarrow$ state `"failed"`.

#### Test D — Manual Replay of Delayed Failed Job
- **Action**: Called `POST /jobs/27/retry` on failed delayed Job 27.
- **Created Job**: **Job 28**
- **Verified Response & Logs**:
  - Preserved original priority: `priority = 2`.
  - Contained parent link metadata: `_replayedFromJobId = "27"`.
  - Reset delay: `delay = 0` (job ran immediately without waiting 10s).
  - Started fresh retry cycle: Reset `attemptsMade` to fresh attempt counter (`Attempt 1 of 3`).

---

### 9. Complete Mental Model Summary

> **Delay controls WHEN a job becomes available.**  
> **Priority controls WHICH waiting job gets selected.**  
> **Retry controls WHAT happens when execution fails.**  
> **Replay creates a NEW job lifecycle.**

- **Retry** = another attempt for the same job.
- **Replay** = a new job based on an old failed job.
- **Priority** = scheduling importance among waiting jobs.
- **Delay** = scheduled wait time before a job becomes eligible for execution.

---

## 20. Idempotency Architecture *(Implemented)*

### 1. What Idempotency Means

**Idempotency** is a mathematical and API design property where performing an operation multiple times produces the exact same result as performing it once.

#### The Real-World Problem of Duplicate Requests
In distributed systems, network connections are inherently unreliable:
1. **Network Retries**: A client submits a request to `POST /jobs`. The server creates the job, but a network drop occurs before the HTTP response reaches the client. The client's HTTP library automatically retries the request.
2. **Double-Clicks & Client Timeouts**: A user clicks "Process Payment" twice, or a mobile client times out after 2 seconds and re-sends the request.

#### Without Idempotency:
- Request 1 $\rightarrow$ Job 30 created (Payment processed)
- Request 2 (Retry) $\rightarrow$ Job 31 created (Payment processed again! ❌ Duplicate charge)

#### With Idempotency:
- Request 1 + `Idempotency-Key: pay-999` $\rightarrow$ Job 30 created (Payment processed)
- Request 2 + `Idempotency-Key: pay-999` $\rightarrow$ Returns existing Job 30 (✅ No duplicate work)

---

### 2. Orchestrix Implementation

Orchestrix implements idempotency directly at the HTTP API layer using **Redis atomic key-value operations**.

#### HTTP Header
Clients pass a unique string in the `Idempotency-Key` header:
```http
POST /jobs
Idempotency-Key: payment-txn-88412
Content-Type: application/json

{
  "task": "process-payment",
  "amount": 500
}
```

#### Redis Storage Architecture
- **Redis Key Namespace**: `idempotency:<Idempotency-Key>`  
  *(e.g., `idempotency:payment-txn-88412`)*
- **Time-To-Live (TTL)**: **24 hours** (`86,400 seconds`).
- **Atomic Reservation Command**:
  ```typescript
  redisClient.set(redisKey, 'PROCESSING', 'EX', 86400, 'NX');
  ```

#### Why `NX` is Critical
The `NX` flag in Redis means **"Set if Not eXists"**. Redis executes this command atomically in its single-threaded event loop. If 10 requests hit the server at the exact same millisecond with the same key, Redis executes `SET ... NX` sequentially:
- **First Request**: Key does not exist $\rightarrow$ Redis sets key $\rightarrow$ Returns `"OK"` (Lock acquired!).
- **Requests 2 through 10**: Key already exists $\rightarrow$ Redis returns `null` (Lock denied!).

#### Two-Phase Key Reservation & Job Creation
1. **Phase 1 (Reservation)**: The winning request atomically sets `idempotency:<key>` to a temporary marker value `"PROCESSING"`.
2. **Phase 2 (Job Creation)**: The winning request calls BullMQ (`taskQueue.add(...)`), receives the generated `jobId` (e.g., `"30"`), and updates the Redis key from `"PROCESSING"` $\rightarrow$ `"30"` (retaining the 24-hour TTL).

#### Safe Job-Creation Failure Cleanup
If BullMQ job creation fails or throws an exception (e.g., Redis connection glitch during `taskQueue.add()`), the `catch` block immediately executes:
```typescript
await redisClient.del(redisKey);
```
This ensures no broken, dangling `"PROCESSING"` lock remains in Redis, allowing subsequent client retries to attempt job creation cleanly.

---

### 3. Request Behavior Matrix

| Request Scenario | HTTP Header | Redis Key Status | HTTP Response | `isDuplicate` | Action Taken |
|---|---|---|---|---|---|
| **First Request with New Key** | `Idempotency-Key: k1` | Does not exist | `201 Created` | `false` | Creates new BullMQ job (e.g., Job 29). |
| **Duplicate Request with Same Key** | `Idempotency-Key: k1` | Points to Job 29 | `200 OK` | `true` | Returns existing Job 29 details; **no new job created**. |
| **Same Key with Different Payload** | `Idempotency-Key: k1` | Points to Job 29 | `200 OK` | `true` | Returns existing Job 29 details; payload in request body is ignored. |
| **Request without Header** | None | Not checked | `201 Created` | `false` | Standard job creation flow continues normally. |

---

### 4. Concurrency & Race-Condition Protection

```
Client Request A ──┐
                   ├─► Compete for SET idempotency:key "PROCESSING" NX EX 86400
Client Request B ──┘
       │
       ├─► Request A wins (returns "OK")  ──► Creates BullMQ Job 41 ──► Updates Redis key to "41"
       │
       └─► Request B loses (returns null) ──► Waits/Polls Redis key ──► Reads Job 41 ──► Returns HTTP 200 OK
```

When multiple simultaneous requests arrive with the exact same `Idempotency-Key`:
1. Both attempt atomic `SET idempotency:<key> "PROCESSING" EX 86400 NX`.
2. **Request A** wins and acquires the lock (`"OK"`). It proceeds to create the job in BullMQ.
3. **Request B** loses (`null`). It enters a non-blocking poll loop (`while (val === 'PROCESSING')`) waiting up to 5 seconds for Request A to complete job creation.
4. As soon as Request A writes the created `jobId` (`"41"`), Request B reads `"41"`, fetches the job from BullMQ, and returns HTTP `200 OK` with `isDuplicate: true`.

---

### 5. Live Testing & Verification Results

The idempotency implementation was verified against Orchestrix through 5 live test suites:

#### Test 1 — First Idempotent Request
- **Request**: `POST /jobs` with `Idempotency-Key: test-key-001`
- **Result**: Returned HTTP `201 Created`, created **Job 29**, `isDuplicate: false`.

#### Test 2 — Duplicate Request
- **Request**: Sent identical `POST /jobs` with `Idempotency-Key: test-key-001`.
- **Result**: Returned HTTP `200 OK`, returned existing **Job 29**, `isDuplicate: true`.

#### Test 3 — Same Key with Different Payload
- **Request**: `POST /jobs` with `Idempotency-Key: test-key-001` and payload `{"task": "different-payload"}`.
- **Result**: Returned HTTP `200 OK`, returned original **Job 29**, preserving original payload without creating a second job.

#### Test 4 — Request Without Header
- **Request**: Standard `POST /jobs` without `Idempotency-Key` header.
- **Result**: Returned HTTP `201 Created`, created new **Job 30**, `isDuplicate: false`.

#### Test 5 — Concurrent Race Condition Benchmark (10 Simultaneous Requests)
- **Method**: Executed 10 parallel asynchronous HTTP POST requests using Node.js `Promise.all()` with identical header `Idempotency-Key: concurrent-key-test`.
- **Result**:
  - **Exactly ONE request** returned HTTP `201 Created` with `isDuplicate: false`.
  - **All NINE other requests** returned HTTP `200 OK` with `isDuplicate: true`.
  - **All 10 responses** referenced the exact same **Job 41**.
- **Conclusion**: Confirmed that atomic Redis `SET NX` reservation successfully eliminates job duplication under high-concurrency race conditions.

---

### 6. Important Mental Model Summary

```
Client
  ↓
POST /jobs + Idempotency-Key
  ↓
Redis checks/reserves key atomically (SET NX)
  ↓
One request creates BullMQ job
  ↓
Redis stores job ID in key
  ↓
Concurrent/repeated requests return the same job
```

> **Idempotency controls WHETHER a new job gets created upon API request.**  
> **Delay controls WHEN a job becomes available.**  
> **Priority controls WHICH waiting job gets selected.**  
> **Retry controls WHAT happens when execution fails.**  
> **Replay creates a NEW job lifecycle.**

---

### 7. Interaction With Existing Orchestrix Features

Idempotency is strictly an **API-level submission protection mechanism**. It operates independently from other queue features:

- **Idempotency vs. Retries**: Idempotency prevents creating duplicate jobs at submission time. Automatic retries execute when a job fails during worker execution. Idempotency does **not** replace worker retries or prevent runtime execution errors.
- **Idempotency vs. Priority**: The initial idempotent request sets the job's priority level. Duplicate requests return the original job with its priority intact.
- **Idempotency vs. Delayed Jobs**: The initial idempotent request sets the job's delay timer. Duplicate requests return the original job in its `delayed` state.
- **Idempotency vs. Manual Replay**: Manual replay (`POST /jobs/:id/retry`) creates a brand-new job cycle to re-run a failed job, intentionally bypassing the original idempotency key.

---

### 8. Untested Edge Cases & Implementation Notes

- **Real-Time 24-Hour Expiration**: The 24-hour key TTL (86,400 seconds) was verified via Redis `TTL idempotency:<key>` inspection commands (returning `~86395s`) rather than waiting 24 real-time hours for automatic Redis key deletion.

---

## 21. Worker Failure & Recovery Architecture *(Implemented)*

### 1. The Worker Failure Problem

In a distributed job queue, background tasks execute inside separate worker processes that run independently from the main API server. A worker process can fail at any time due to:
- Operating system crashes or unhandled native exceptions.
- Hard process terminations (`SIGKILL`, `process.exit(1)`).
- Machine power loss or cloud container eviction.

#### The Core Vulnerability
When a worker process crashes mid-execution:
1. The task logic stops running immediately.
2. No error handling block is reached, so the worker cannot report a failure to Redis.
3. The job remains left in BullMQ's **`active`** state in Redis.

Without a recovery mechanism, that job would remain orphaned in `active` state forever, uncompleted and un-retried.

---

### 2. BullMQ Job Locks

To coordinate multiple workers cleanly, BullMQ ensures that no two workers process the exact same queued job concurrently using **Redis Job Locks**.

#### Lock Mechanics (`lockDuration`)
- When a worker picks up a job from `waiting`, BullMQ creates a lock entry in Redis (`bull:orchestrix-queue:<jobId>:lock`) bound to that worker's unique ID.
- While the worker process is healthy and actively executing, it periodically sends a heartbeat to Redis to renew/extend this lock timer.
- Orchestrix is configured with:
  ```typescript
  const lockDuration = parseInt(process.env.WORKER_LOCK_DURATION || '5000', 10); // 5000ms (5 seconds)
  ```
- **Why Locks Expire**: If a worker process crashes abruptly, its heartbeat loop dies instantly. It can no longer send renewal commands to Redis. After 5 seconds (`lockDuration`), the Redis key lock automatically expires.

---

### 3. Stalled Job Detection & Recovery

A **Stalled Job** is an active job in Redis whose worker lock has expired without being renewed.

#### Stalled Checking (`stalledInterval`)
- Healthy worker processes periodically inspect Redis for active jobs whose lock keys have expired.
- Orchestrix is configured with:
  ```typescript
  const stalledInterval = parseInt(process.env.WORKER_STALLED_INTERVAL || '5000', 10); // 5000ms (5 seconds)
  ```
- Every 5 seconds, active workers check Redis for orphaned active jobs.
- When an expired lock is detected, BullMQ emits the `stalled` event and automatically moves the job from `active` back to `waiting`.
- Orchestrix logs this recovery event using a dedicated listener in `src/worker.ts`:
  ```typescript
  worker.on('stalled', (jobId, prev) => {
    console.warn(
      `[Worker ${workerPid}] ⚠️ [Event] Stalled job detected! Job ID ${jobId} (previous state: ${prev}) was left active by a crashed worker and has been recovered for re-execution.`
    );
  });
  ```

---

### 4. Preventing Endless Crash Loops (`maxStalledCount`)

If a job contains bad data or a bug that causes the worker process itself to crash every time it runs (a "poison pill" job), recovering the job infinitely would crash every worker process in your cluster one after another.

#### Stalled Count Threshold (`maxStalledCount`)
Orchestrix configures:
```typescript
const maxStalledCount = parseInt(process.env.WORKER_MAX_STALLED_COUNT || '1', 10); // 1 stall allowed
```
- `maxStalledCount = 1` dictates that a job can be recovered from a worker stall **at most once**.
- If a job stalls a second time (because the recovered worker also crashed), BullMQ stops recovering it, marks the job state as **`failed`**, and sets the failure reason to:
  `job stalled more than allowable limit`

---

### 5. Local Crash Simulation (`simulate-crash`)

To test worker process failure without depending on timing luck, `src/worker.ts` includes a dedicated crash testing trigger:

```typescript
if (job.data && job.data.task === 'simulate-crash') {
  console.warn(`[Worker ${workerPid}] 💥 Simulating abrupt WORKER PROCESS CRASH for Job ID: ${job.id}`);
  await delay(1000); // Allow job state to transition to active in Redis
  process.exit(1);   // Abruptly terminate OS worker process
}
```

> **Note**: This is strictly a local simulation mechanism for testing and does not represent normal job processing.

---

### 6. Long-Running Job Simulation (`simulate-long-job`)

To enable deterministic manual worker termination tests (allowing an operator enough time to manually kill a worker process via terminal/Task Manager while a job is actively running), `src/worker.ts` supports:

```typescript
if (job.data && job.data.task === 'simulate-long-job') {
  console.log(`[Worker ${workerPid}] ⏳ Started long-running test job for Job ID: ${job.id} (Executing for 60s)...`);
  await delay(60000); // Keeps job active for 60 seconds
}
```

- **Duration**: Executes for **60 seconds** (`await delay(60000)`).
- **Purpose**: Gives the developer plenty of time to kill the active worker process manually.
- **Normal Jobs**: Standard jobs continue to complete in **3 seconds**.

---

### 7. Verified Recovery Test (Job ID 51)

We verified end-to-end worker failure and recovery with a live test on Orchestrix:

1. **Job Submission**:
   - Submitted a 60-second test job: `POST /jobs` with `{"task": "simulate-long-job"}`.
   - API returned **Job ID `51`**.

2. **Initial Execution & Crash**:
   - **Worker Process #1 (PID 20544)** picked up Job 51 and logged:
     `[Worker 20544] 🚀 Started Job ID: 51`
   - While Job 51 was actively running ($t \approx 10\text{s}$), Worker Process 20544 was manually terminated (`CTRL+C`).
   - Job 51 remained stuck in `active` state in Redis with an un-renewed lock.

3. **Stalled Detection & Recovery**:
   - Started a new healthy worker instance: **Worker Process #2 (PID 30864)**.
   - After lock expiration (~5s), Worker 30864 detected Job 51 was stalled and logged:
     `[Worker 30864] ⚠️ [Event] Stalled job detected! Job ID 51 (previous state: active)... recovered for re-execution.`

4. **Successful Completion**:
   - Worker 30864 automatically re-executed Job 51 for 60 seconds and completed it successfully.
   - Querying `GET /jobs/51` returned:
     - `state: "completed"`
     - `attemptsMade: 1`
     - `returnvalue.workerPid: 30864`
     - `returnvalue.executionTimeMs: ~60000`

#### Proved Workflow:
$$\text{Worker Crash} \longrightarrow \text{Lock Expired} \longrightarrow \text{Stalled Detected} \longrightarrow \text{Healthy Worker Recovers} \longrightarrow \text{Job Completed}$$

---

### 8. Failed Crash-Loop Test (Job ID 42 & `maxStalledCount`)

During earlier testing with `simulate-crash` (Job ID `42`), we verified how `maxStalledCount = 1` prevents infinite crash loops:

1. **Worker #1 (PID 1234)** picked up Job 42 (`task: "simulate-crash"`), waited 1s, and called `process.exit(1)` $\rightarrow$ **Crashed**.
2. **Worker #2 (PID 5678)** detected Job 42 as stalled, recovered it, and re-executed it.
3. Because the payload still contained `"task": "simulate-crash"`, **Worker #2 also crashed** (`process.exit(1)`).
4. Because `maxStalledCount` was set to `1`, BullMQ stopped attempting further recoveries, set Job 42's state to **`failed`**, and recorded:
   `failedReason: "job stalled more than allowable limit"`

This test confirmed that `maxStalledCount` successfully prevents poison-pill jobs from bringing down an entire worker cluster indefinitely.

---

### 9. Critical Architectural Distinction

It is important to understand the fundamental difference between an **Application/Job Failure** and a **Worker/Process Failure**:

| Feature | Application / Job Failure | Worker / Process Failure |
|---|---|---|
| **What Happened?** | Job processor code threw an exception (`throw new Error(...)`). | Worker OS process died abruptly (`process.exit(1)`, `SIGKILL`). |
| **Worker State** | Worker process remains **alive** and healthy. | Worker process is **dead** / disappeared. |
| **Job Lock** | Worker explicitly releases lock and reports failure to Redis. | Lock is orphaned; automatically expires after `lockDuration` (5s). |
| **Handling Mechanism** | **Automatic Retries & Exponential Backoff** (`attempts: 3`, backoff 2s/4s). | **Stalled Job Recovery** (`stalledInterval`, `maxStalledCount`). |
| **Retry Counter** | `attemptsMade` increments ($1 \rightarrow 2 \rightarrow 3$). | Job is moved back to `waiting` for another worker to pick up. |

---

### 10. Interaction With Existing Features

Worker Failure & Recovery is a **worker-infrastructure resilience mechanism** that operates underneath all other features:

- **Recovery vs. Automatic Retries**: Retries handle code exceptions inside a running worker. Worker recovery handles the total death of the worker process itself.
- **Recovery vs. Priority**: When a stalled job is recovered and moved back to `waiting`, BullMQ respects its original `priority` when a worker picks it up next.
- **Recovery vs. Delayed Jobs**: If a worker crashes while processing a delayed job (after its delay timer elapsed), stalled recovery re-queues it cleanly.
- **Recovery vs. Idempotency**: Idempotency prevents duplicate job submissions at the API layer. Worker recovery ensures submitted jobs are processed to completion even if workers die.
- **Recovery vs. Manual Replay**: Manual replay is an explicit API request to re-run a failed job cycle. Worker recovery happens automatically in the background without user intervention.

---

### 11. Mental Model Summary

```
API
  ↓
Redis / BullMQ
  ↓
Worker A
  ↓
Job becomes active + lock is renewed
  ↓
Worker A crashes 💥
  ↓
Lock expires (5s)
  ↓
BullMQ detects stalled job ⚠️
  ↓
Worker B recovers job
  ↓
Job completes 🎉
```

> **Worker recovery controls WHAT happens when a worker process crashes mid-job.**  
> **Idempotency controls WHETHER a new job gets created upon API request.**  
> **Delay controls WHEN a job becomes available.**  
> **Priority controls WHICH waiting job gets selected.**  
> **Retry controls WHAT happens when execution fails.**  
> **Replay creates a NEW job lifecycle.**

---

## 11. Feature 8: Durable Job Metadata & History Storage (PostgreSQL 16)

### 1. Why PostgreSQL Was Added Alongside Redis/BullMQ

Redis is an in-memory database designed for microsecond response times and atomic data structure operations. However, in-memory data structures are ephemeral:
- Once BullMQ jobs complete, they are eventually pruned or flushed.
- Redis memory is volatile and not optimized for complex SQL reporting, long-term auditing, historical trend analysis, or cross-job relational queries.

Adding **PostgreSQL 16** (`orchestrix-postgres`) gives Orchestrix a permanent, searchable, application-level audit history store that outlives Redis memory flushes and container restarts.

---

### 2. Separation of Responsibilities

Orchestrix strictly decouples queue execution from historical data persistence:

| Concern | Primary Storage | Responsibility |
|---|---|---|
| **Queue Mechanics & Real-Time State** | **Redis / BullMQ** | Lock acquisition, job state transitions (`waiting`, `active`, `delayed`), priority scheduling, exponential backoff timers, and worker stalled detection. |
| **Durable Audit & Historical Analytics** | **PostgreSQL** | Permanent job log, execution duration (`execution_time_ms`), worker PID attribution, failure reason history, and manual replay lineage (`replayed_from_job_id`). |

> **Crucial Rule**: Redis/BullMQ remains the sole source of truth for active queue execution. PostgreSQL is an **application-level audit log** and does **NOT** manage or block queue mechanics.

---

### 3. PostgreSQL `jobs` Schema & Field Semantics

```sql
CREATE TABLE IF NOT EXISTS jobs (
  id                    BIGSERIAL    PRIMARY KEY,
  bullmq_job_id         TEXT         NOT NULL UNIQUE,
  task_type             TEXT         NOT NULL,
  status                TEXT         NOT NULL DEFAULT 'pending',
  priority              INTEGER      NOT NULL DEFAULT 5,
  attempts              INTEGER      NOT NULL DEFAULT 0,
  max_attempts          INTEGER      NOT NULL DEFAULT 3,
  worker_id             TEXT,
  created_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  started_at            TIMESTAMPTZ,
  completed_at          TIMESTAMPTZ,
  failed_at             TIMESTAMPTZ,
  execution_time_ms     INTEGER,
  failed_reason         TEXT,
  replayed_from_job_id  TEXT         REFERENCES jobs(bullmq_job_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_status     ON jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_bullmq_id  ON jobs (bullmq_job_id);
```

#### Key Field Explanations:
- `bullmq_job_id` (`TEXT UNIQUE`): Maps directly to BullMQ's generated string ID (e.g., `"52"`), allowing fast cross-system correlation.
- `status` (`TEXT`): Captures granular lifecycle states (`pending`, `running`, `retrying`, `completed`, `failed`, `stalled`).
- `attempts` (`INTEGER`): Tracks BullMQ's authoritative attempt counter (`job.attemptsMade`).
- `worker_id` (`TEXT`): Records the OS process PID (`process.pid`) of the worker that executed the job.
- `execution_time_ms` (`INTEGER`): Real duration of execution in milliseconds, calculated in `worker.ts`.
- `replayed_from_job_id` (`TEXT FK`): Points to the parent job's `bullmq_job_id` when a job is created via manual replay (`POST /jobs/:id/retry`).

---

### 4. PostgreSQL Client Selection: `pg` (node-postgres) & Raw SQL

We selected **`node-postgres` (`pg`)** with raw parameterized SQL queries over ORMs (such as Prisma or TypeORM):
- **Lightweight & Fast**: Zero build-time code generation step required. Works seamlessly with `ts-node` and `tsc`.
- **Explicit Querying**: Direct SQL control prevents hidden query overhead or unexpected ORM hooks that could interfere with high-throughput queue events.
- **Connection Pooling**: Uses `pg.Pool` with environment variables (`DATABASE_HOST`, `DATABASE_PORT`, `DATABASE_NAME`, `DATABASE_USER`, `DATABASE_PASSWORD`) defaulting to the container environment.

---

### 5. Best-Effort Database Initialization (`initDb()`)

`src/db.ts` exposes `initDb()`, which is called inside an async IIFE during module startup in both `api.ts` and `worker.ts`:

```typescript
export async function initDb(): Promise<void> {
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS jobs (...)`);
    // Create indexes...
    console.log('[DB] ✅ PostgreSQL: jobs table ready');
  } catch (error) {
    console.error('[DB] ⚠️ PostgreSQL initialization failed (server/worker will continue running without DB persistence):', error);
  }
}
```

- **Resilience**: If PostgreSQL is temporarily down or unreachable during container startup, `initDb()` logs a clear warning without crashing the Express server or Worker process.
- **Independence**: Redis/BullMQ remains 100% operational regardless of PostgreSQL availability.

---

### 6. Job Lifecycle Transitions in PostgreSQL

#### Normal Lifecycle:
$$\text{API POST /jobs} \xrightarrow{\text{status='pending'}} \text{Worker Starts} \xrightarrow{\text{status='running'}} \text{Worker Finish} \xrightarrow{\text{status='completed'}}$$

#### Automatic Retry Lifecycle:
$$\text{Attempt 1 Fails} \xrightarrow{\text{status='retrying', attempts=1}} \text{Attempt 2 Fails} \xrightarrow{\text{status='retrying', attempts=2}} \text{Attempt 3 Fails} \xrightarrow{\text{status='failed', attempts=3}}$$

#### Manual Replay Representation:
1. Original job row remains in `status = 'failed'` permanently as an immutable audit record.
2. `POST /jobs/:id/retry` creates a **new BullMQ job** and inserts a **new PostgreSQL row** with `replayed_from_job_id` set to the original job's `bullmq_job_id`.

#### Worker Crash / Stalled Recovery Lifecycle:
$$\text{Worker A running} \xrightarrow{\text{Worker A Crashes}} \text{Lock Expired} \xrightarrow{\text{stalled Event (status='stalled')}} \text{Worker B Recovers} \xrightarrow{\text{status='running'}} \text{status='completed'}$$

> **Key Rule on Stall Recovery**: A worker stall detection event updates `status = 'stalled'` in PostgreSQL, but **does not increment `attempts`**. BullMQ does not consider a worker crash a job code failure, so `attemptsMade` remains unchanged until the recovered worker executes or fails.

---

### 7. Non-Blocking / Best-Effort Writes

Every database operation in `api.ts` and `worker.ts` is wrapped in an isolated `try/catch` block:

```typescript
try {
  await updateJobCompleted({...});
} catch (dbError) {
  console.error(`[DB] ❌ Failed to update completed job ${job.id} in PostgreSQL:`, dbError);
  // Do NOT rethrow — BullMQ queue execution remains unaffected
}
```

If a PostgreSQL write query times out or fails:
1. The error is logged to stdout/stderr.
2. The BullMQ job execution, retry logic, and HTTP response continue without interruption.

---

### 8. End-to-End Test Verifications

We performed comprehensive live integration testing against PostgreSQL 16:

#### Test 1: Successful Job Lifecycle (Job ID 52)
- **Action**: Submitted standard job via `POST /jobs`.
- **Database Result**: Row created with `status = 'completed'`, `attempts = 1`, `worker_id = 16232`, `execution_time_ms = 3016`, `completed_at` populated.

#### Test 2: Automatic Retries & Final Failure (Job ID 53)
- **Action**: Submitted failing job (`task: "simulate-failure"`, `attempts: 3`).
- **Database Result**:
  - Attempt 1: Updated to `status = 'retrying'`, `attempts = 1`.
  - Attempt 2: Updated to `status = 'retrying'`, `attempts = 2`.
  - Attempt 3: Updated to `status = 'failed'`, `attempts = 3`, `failed_at` populated, `failed_reason = "Simulated failure on attempt 3 of 3"`.

#### Test 3: Manual Replay Lineage (Job ID 54)
- **Action**: Triggered manual replay via `POST /jobs/53/retry`.
- **Database Result**:
  - Original Job 53 remained `status = 'failed'` (immutable).
  - New Job 54 created with `status = 'completed'`, `replayed_from_job_id = "53"`.

#### Test 4: Worker Crash & Stalled Recovery (Job ID 60)
- **Action**: Submitted 60-second test job (`task: "simulate-long-job"`). Worker PID `16232` started Job 60. Worker `16232` was forcefully killed (`CTRL+C`). Healthy Worker PID `14708` recovered Job 60 after lock expiration.
- **Database Result**:
  - **Initial State**: Updated to `running` by Worker `16232`.
  - **Crash & Recovered State**: `stalled` event fired, then Worker `14708` updated status to `running`.
  - **Final State**: `status = 'completed'`, `attempts = 1`, `worker_id = "14708"`, `execution_time_ms = 60019`.
  - **Important Observation**: The final PostgreSQL record correctly recorded the recovering worker PID (`14708`) and completed successfully with `attempts = 1`, demonstrating that worker crash recovery does not inflate job execution attempt counts.

---

---

## 15. Dead Letter Queue (DLQ) & Failure Handling

### 1. Why a Dedicated DLQ Was Introduced
In high-throughput queue systems, jobs that fail permanently after exhausting all automatic retry attempts can clutter operational queue metrics and risk accidental re-processing if left in the main queue's `failed` state. 

We introduced a dedicated **Dead Letter Queue (`orchestrix-dlq`)** to serve as an isolated, passive sink for permanently failed jobs. This separates active operational queue management in `orchestrix-queue` from dead job triage, inspection, auditing, and manual replay workflows.

---

### 2. Failure Lifecycle Overview

```
Job Submitted (POST /jobs)
     │
     ▼
[orchestrix-queue] ──(Attempt 1 Fails)──► Exponential Backoff Delay
     │                                                │
     │◄─────────────────(Attempt 2 Retry)─────────────┘
     │
     ├─(Attempt 3 / Max Attempts Exhausted)
     │
     ▼
[Worker detect permanent failure]
     │
     ├─► 1. Create Snapshot Entry in `orchestrix-dlq` (jobId: "dlq-62")
     │
     └─► 2. Update PostgreSQL status from 'failed' to 'dead_lettered'
```

---

### 3. Dedicated Passive BullMQ Queue (`orchestrix-dlq`)
- **Queue Instance**: `export const dlqQueue = new Queue('orchestrix-dlq', { connection: redisConnection })`.
- **Passive Nature**: **No worker process is attached to `orchestrix-dlq`**. Jobs sit passively in Redis and will never be automatically picked up or executed.

---

### 4. BullMQ ID Compatibility Bug & Resolution
Initially, we attempted to format DLQ job IDs as `dlq:${originalJobId}` (e.g. `dlq:62`). However, BullMQ threw an immediate runtime validation error:

```
Error: Custom Id cannot contain :
```

**Resolution**: The deterministic DLQ job ID format was corrected to **`dlq-${originalJobId}`** (e.g., `dlq-62`), using a hyphen instead of a colon.

---

### 5. DLQ Snapshot Structure
When a job permanently fails, a snapshot object is stored as a job in `orchestrix-dlq`:

```json
{
  "originalJobId": "62",
  "taskType": "default-job",
  "payload": {
    "task": "simulate-failure"
  },
  "failedReason": "Simulated failure on attempt 3 of 3",
  "attemptsMade": 3,
  "maxAttempts": 3,
  "priority": 5,
  "failedAt": "2026-09-28T16:45:00.000Z"
}
```

---

### 6. PostgreSQL Status Transition (`failed` → `dead_lettered`)
- **PostgreSQL Column**: `status` is a `TEXT` column (no enum schema migration was needed).
- **Enforced Sequence**:
  1. When a job fails its final attempt, `updateJobFailed` sets status to `'failed'` in PostgreSQL.
  2. Worker adds the snapshot to `orchestrix-dlq` (`jobId: "dlq-62"`).
  3. **Only after** the DLQ snapshot is verified present, `updateJobDeadLettered` updates status in PostgreSQL from `'failed'` to `'dead_lettered'`.

---

### 7. Idempotent DLQ Insertion & Error Resilience
- **Idempotency**: Worker uses deterministic custom job ID `dlq-${originalJobId}` when adding to `dlqQueue`. If a duplicate insertion occurs (e.g., event re-fire), the worker catches the duplicate error, verifies existence via `dlqQueue.getJob(dlqJobId)`, and treats the operation as successful.
- **Worker Crash Protection**: If Redis or PostgreSQL updates fail during DLQ handling, the error is caught and logged (`[DLQ] ❌ ...`). **The worker process never crashes due to a secondary DLQ logging failure.**

---

### 8. Manual Replay Mechanics (`POST /jobs/:id/retry`)
- **Canonical Lineage**: Replaying a dead-lettered job accepts the original job ID (e.g., `62` or `dlq-62`), extracts the original payload & priority, and adds a **completely NEW job** (e.g., Job `64`) into `orchestrix-queue`.
- **PostgreSQL Linkage**: The new job's PostgreSQL record references the original numerical ID:
  ```sql
  replayed_from_job_id = '62'
  ```
  *(Never stores `"dlq-62"` as the database lineage ID)*.
- **DLQ Snapshot Preservation**: Triggering manual replay does **NOT** delete the original DLQ snapshot or alter the original PostgreSQL `'dead_lettered'` row.

---

### 9. REST API Endpoints for DLQ Governance

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/dlq` | Returns all job snapshots sitting in the Dead Letter Queue. |
| `GET` | `/dlq/:id` | Returns specific DLQ job details (accepts `62` or `dlq-62`). Uses direct lookup and fallback scan to guarantee resolution. |
| `DELETE` | `/dlq/:id` | Removes snapshot entry from `orchestrix-dlq`. **Original PostgreSQL execution history remains 100% intact.** |
| `POST` | `/jobs/:id/retry` | Replays a failed or dead-lettered job by creating a new job cycle in `orchestrix-queue`. |

---

### 10. Operational Engine vs. Durable System of Record

```
              ┌─────────────────────────────────────────────────┐
              │                Client / Worker                  │
              └────────────────────────┬────────────────────────┘
                                       │
            ┌──────────────────────────┴──────────────────────────┐
            ▼                                                     ▼
┌─────────────────────────┐                             ┌──────────────────┐
│     Redis / BullMQ      │                             │    PostgreSQL    │
│   (Operational Engine)  │                             │ (Durable Record) │
├─────────────────────────┤                             ├──────────────────┤
│ orchestrix-queue        │                             │ Table: jobs      │
│   ├─ active             │                             │   ├─ status:     │
│   ├─ waiting            │                             │   │  'pending'   │
│   └─ completed          │                             │   │  'running'   │
│                         │                             │   │  'retrying'  │
│ orchestrix-dlq (Passive)│                             │   │  'completed' │
│   └─ dlq-62 (Snapshot)  │                             │   │  'dead_lettered'│
└─────────────────────────┘                             └──────────────────┘
```

- **BullMQ (`orchestrix-queue` & `orchestrix-dlq`)**: Manages operational execution, active concurrency locks, exponential backoff timers, and isolated passive snapshot storage.
- **PostgreSQL**: Durable audit trail recording lifecycle timestamps, execution timing, worker PIDs, error reasons, and replay lineage (`replayed_from_job_id`).

---

### 11. End-to-End Live Integration Test Verification

We performed full end-to-end integration testing of the DLQ subsystem:

1. **Permanent Failure & DLQ Relocation (Job ID 62)**:
   - Submitted failing job with `maxAttempts: 3`.
   - Worker attempted execution 3 times with backoff delays.
   - Worker created snapshot `dlq-62` in `orchestrix-dlq`.
   - `GET /dlq` returned `{ count: 1, dlqJobs: [{ id: "dlq-62", ... }] }`.
   - `GET /dlq/62` returned the full snapshot details.
   - PostgreSQL record for Job 62 updated to `status = 'dead_lettered'`, `attempts = 3`.

2. **Manual Replay of Dead-Lettered Job (Job ID 64)**:
   - Called `POST /jobs/62/retry`.
   - New Job 64 was enqueued into `orchestrix-queue` (starting at Attempt 1 of 3).
   - Original Job 62 remained `status = 'dead_lettered'` in PostgreSQL and `dlq-62` remained in DLQ.
   - Job 64 PostgreSQL record showed `replayed_from_job_id = '62'`.
   - Job 64 (configured with simulated failure) executed 3 attempts and moved into DLQ as `dlq-64`.

3. **DLQ Snapshot Cleanup**:
   - Called `DELETE /dlq/64`.
   - Snapshot `dlq-64` was removed from `orchestrix-dlq`.
   - `GET /dlq/64` returned `404 Not Found`.
   - PostgreSQL row for Job 64 remained completely intact with `status = 'dead_lettered'`.

---

## 30. Job Dependencies & Workflows (DAGs)

### 1. Why Dependencies are Needed
In complex data pipelines and distributed background workflows (e.g., ETL pipelines, order fulfillment, report generation), background tasks often depend on prior steps finishing successfully:
- **Sequential Execution**: Task B (e.g., `process-data`) must only run after Task A (e.g., `fetch-data`) completes.
- **Data Integrity**: Task C (e.g., `generate-report`) must not execute on partial or unverified data if Task B failed or dead-lettered.
- **Failure Short-Circuiting**: If an upstream step fails permanently or is dead-lettered, downstream tasks must immediately block and prevent unnecessary computation or corrupted output.

### 2. Architecture & Design Principles
- **Declarative DAG Specification**: Users can define individual job dependencies (`POST /jobs/:id/dependencies`) or submit entire Directed Acyclic Graphs (DAGs) via `POST /workflows`.
- **PostgreSQL System of Record**: Dependency links are stored durably in `job_dependencies` with strict foreign keys to `jobs(bullmq_job_id)`.
- **Worker Pre-Check Evaluation**: Before executing any job payload, the worker evaluates upstream dependencies via `evaluateJobDependencies(jobId)`:
  - If all upstream dependencies are `completed`: job proceeds to execution.
  - If any upstream dependency is `pending`, `running`, or `retrying`: worker defers job execution safely.
  - If any upstream dependency is `failed` or `dead_lettered`: job execution is blocked and aborted immediately.

### 3. PostgreSQL Schema (`job_dependencies`)
```sql
CREATE TABLE IF NOT EXISTS job_dependencies (
  id                 BIGSERIAL    PRIMARY KEY,
  job_id             TEXT         NOT NULL REFERENCES jobs(bullmq_job_id) ON DELETE CASCADE,
  depends_on_job_id  TEXT         NOT NULL REFERENCES jobs(bullmq_job_id) ON DELETE CASCADE,
  created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  CONSTRAINT unique_job_dependency UNIQUE (job_id, depends_on_job_id)
);

CREATE INDEX IF NOT EXISTS idx_job_deps_job_id ON job_dependencies (job_id);
CREATE INDEX IF NOT EXISTS idx_job_deps_depends_on ON job_dependencies (depends_on_job_id);
```

### 4. Execution Lifecycle & Dependency Resolution
```
1. Client POST /workflows → Creates Job A, Job B, Job C & inserts job_dependencies:
   - Job B depends on Job A
   - Job C depends on Job B

2. Worker picks up Job B:
   - Evaluates evaluateJobDependencies(Job B)
   - Sees Job A is 'pending' / 'running'
   - Defers Job B for 2000ms

3. Worker completes Job A → Status in DB updated to 'completed'

4. Worker picks up deferred Job B:
   - Evaluates evaluateJobDependencies(Job B)
   - Sees Job A is 'completed'
   - Executes Job B → Status updated to 'completed'

5. Worker picks up deferred Job C:
   - Evaluates evaluateJobDependencies(Job C)
   - Sees Job B is 'completed'
   - Executes Job C → Status updated to 'completed'
```

### 5. Failure Behavior & DLQ Protection
- If an upstream job (e.g., Job A) fails all max attempts and moves to status `failed` or `dead_lettered`:
- When Worker picks up Job B, `evaluateJobDependencies(Job B)` detects the upstream failure.
- Worker immediately aborts execution of Job B and marks it blocked due to dependency failure.
- Downstream jobs (Job C) are also protected from executing.

### 6. Circular Dependency & Validation Rules
To ensure workflow graph stability:
- **Self-Dependency Rejection**: A job cannot depend on itself (`jobId === dependsOnJobId` returns `400 Bad Request`).
- **Recursive Cycle Detection**: `hasCircularDependency(jobId, dependsOnJobId)` uses a PostgreSQL Recursive CTE to trace dependency paths. If a path already exists from `dependsOnJobId` to `jobId`, the request is rejected with `400 Bad Request`.
- **DAG Workflow Validation**: `POST /workflows` uses Kahn's algorithm / Topological Sorting on the incoming job graph to validate DAG properties before any job or dependency is written to Redis or PostgreSQL.
- **Duplicate Prevention**: `unique_job_dependency` constraint prevents duplicate dependency records.

### 7. End-to-End Integration Testing Verification
We verified all dependency requirements:
1. **A → B Successful Execution**: Job A completed, Job B waited for A and executed successfully.
2. **Permanent Failure Short-Circuit**: Job A configured with simulated failure; Job B was blocked and did not execute.
3. **A → B → C Chain**: Multi-tier dependency chain executed in exact topological order.
4. **Circular & Duplicate Rejection**: `POST /jobs/:id/dependencies` rejected self-dependencies, duplicate dependencies (`409 Conflict`), and circular dependencies (`400 Bad Request`).
5. **System Compatibility**: Verified normal jobs, recurring schedules, DLQ snapshots, priority queues, and manual replays continue working without regression.

---

## 31. Observability & Metrics

### 1. Architecture: Real-time vs. Durable Separation
Orchestrix maintains a strict separation of concerns for operational observability:
- **Redis / BullMQ (Real-time Operational State)**:
  - Waiting, active, delayed, and failed queue counts
  - Passive Dead Letter Queue (DLQ) snapshot count
  - Oldest waiting job age calculation
  - Active worker heartbeats and process liveness
- **PostgreSQL (Durable Historical System of Record)**:
  - Lifetime job totals, completed count, failed count, and dead-lettered count
  - Execution time averages (`avgExecutionTimeMs`)
  - Total attempt counters and retries
  - Recurring schedule performance (active, paused, total executions, next run timestamps)
  - Workflow & DAG dependency completion stats
  - Historical execution breakdown by worker PID

### 2. Worker Heartbeat & Auto-Expiration
- **Heartbeat Registration**: Each active worker writes metadata to Redis under `worker:heartbeat:<PID>`.
- **Heartbeat Interval**: 5 seconds.
- **Redis Key TTL**: 15 seconds (refreshed on every heartbeat).
- **Heartbeat Metadata Payload**:
  - `workerPid`: Process ID of worker.
  - `startTime`: ISO timestamp when worker initialized.
  - `concurrency`: Worker job concurrency setting.
  - `lastHeartbeat`: ISO timestamp of latest heartbeat.
- **Graceful Shutdown**: On `SIGINT` or `SIGTERM`, worker explicitly removes `worker:heartbeat:<PID>` from Redis.
- **Unexpected Crash Handling**: If a worker process crashes, its Redis heartbeat key automatically expires after 15 seconds, preventing crashed workers from appearing healthy.

### 3. Non-blocking Worker Discovery
To prevent Redis event loop blocking in production environments with large keyspaces, `/metrics/workers` utilizes non-blocking `SCAN` iteration (`MATCH worker:heartbeat:*`) with cursor pagination rather than dangerous `KEYS` commands.

### 4. Metrics API Specification

| Endpoint | Data Source | Description |
|---|---|---|
| `GET /metrics` | Redis + PostgreSQL | Master endpoint returning full unified system metrics summary with DB connection state indicator. |
| `GET /metrics/jobs` | PostgreSQL | Historical job execution metrics: total, completed, failed, dead-lettered, running, pending, retrying, avg execution time, total attempts, and success/failure rates. |
| `GET /metrics/queue` | BullMQ / Redis | Real-time queue metrics: waiting, active, delayed, failed, DLQ snapshot count, oldest waiting job age, and throughput. |
| `GET /metrics/workers` | Redis + PostgreSQL | Active worker process count, PID details, uptime, concurrency, last heartbeat, and historical job execution breakdown per worker. |
| `GET /metrics/scheduler` | PostgreSQL | Recurring schedule metrics: total schedules, active vs. paused count, total runs, last run, and next scheduled run timestamps. |
| `GET /metrics/workflows` | PostgreSQL | DAG workflow metrics: count of jobs with dependencies, total dependency relationships, completed dependent jobs, failed dependent jobs, and blocked job count. |

### 5. PostgreSQL Optimization & SQL Aggregates
- **Worker Indexing**: Added `CREATE INDEX IF NOT EXISTS idx_jobs_worker_id ON jobs (worker_id)` to enable fast per-worker job execution aggregation.
- **Single SQL Aggregate Queries**: Avoided iterative querying by using conditional SQL aggregation (`COUNT(*) FILTER (...)`) to fetch complete job lifecycle breakdowns in a single query pass.

### 6. Fault Tolerance & DB Downtime Resilience
Metrics collection is non-disruptive and isolated from core job processing. If PostgreSQL is temporarily unreachable:
- `/metrics` gracefully returns active operational metrics from Redis/BullMQ.
- Sets `"dbConnected": false` in the response body rather than throwing a 500 error or crashing the server.

### 7. Verification & Verification Steps
The Observability & Metrics feature was verified:
1. `npm run build` → Successful compilation with zero TypeScript errors.
2. Master `/metrics` endpoint → Returns clean JSON structure aggregating all sub-systems.
3. `/metrics/workers` → Correctly lists active worker PID, concurrency, uptime, and last heartbeat timestamp.
4. Graceful worker shutdown (`SIGINT`/`SIGTERM`) → Heartbeat key deleted immediately from Redis.
5. Forced worker termination (Crash simulation) → Heartbeat key automatically expires after 15-second Redis TTL.
6. Core Orchestrix functionality → Queue processing, retries, exponential backoff, priority, idempotency, stalled recovery, DLQ, manual replay, recurring schedules, and workflows continue working without performance degradation.

---

### Architectural Mental Model

```
 ┌─────────────────────────────────────────────────────────────┐
 │                Redis / BullMQ Operational                   │
 │             (Real-Time Execution & Liveness)                │
 └──────────────────────────────┬──────────────────────────────┘
                                │
                                ▼
 ┌─────────────────────────────────────────────────────────────┐
 │                   Unified Metrics APIs                      │
 │                     (GET /metrics/*)                        │
 └──────────────────────────────▲──────────────────────────────┘
                                │
 ┌──────────────────────────────┴──────────────────────────────┐
 │                PostgreSQL System of Record                  │
 │              (Durable Historical Audit Log)                 │
 └─────────────────────────────────────────────────────────────┘
```

- **Redis / BullMQ** = Real-time execution state
- **PostgreSQL** = Durable historical system of record
- **Metrics API** = Unified observability layer

---

## 32. Batch Job Processing & Batch Lifecycle

### 1. Purpose
Batch processing was added to allow a client to submit multiple jobs together as a single named logical unit — a **batch** — while each job in that batch remains a fully independent BullMQ job internally.

Before this feature, a client submitting ten related jobs had no way to observe them as a group, track combined progress, or determine whether the logical operation as a whole succeeded or partially failed. The batch layer adds that observability and grouping without changing how individual jobs are executed, retried, or dead-lettered.

### 2. Architecture

```
Client
  │
  ▼
POST /jobs/bulk
  │
  ├── PostgreSQL ────────────────────────────────────────────────┐
  │     Creates batch record (job_batches)                       │
  │     Creates individual job records (jobs.batch_id = N)       │
  │                                                              │
  └── BullMQ / Redis ────────────────────────────────────────────┤
        Enqueues each job as a normal BullMQ job                 │
                                                                 │
              Worker picks up individual jobs                    │
                │                                                │
                ├── job completes   → PostgreSQL updated ────────┤
                ├── job retries     → BullMQ reschedules         │
                └── job dead-lettered → DLQ + PostgreSQL ────────┘
                                               │
                                    updateBatchProgress()
                                    recalculates batch status
                                    from actual job states
```

**Strict layer separation:**
- **Redis / BullMQ** is the execution and queue layer. Workers read from Redis and know nothing about batches.
- **PostgreSQL** is the durable batch and job history layer. Batch status is derived entirely from actual job outcomes stored in PostgreSQL — never trusted from client input.

No second queue or batch worker was introduced. The batch layer is purely an orchestration and tracking layer above the existing individual-job system.

### 3. PostgreSQL Schema

#### `job_batches` table
Stores durable batch metadata. Created idempotently at server startup alongside the existing tables.

```sql
CREATE TABLE IF NOT EXISTS job_batches (
  id                 BIGSERIAL    PRIMARY KEY,
  name               TEXT         NOT NULL,
  status             TEXT         NOT NULL DEFAULT 'pending',
  total_jobs         INTEGER      NOT NULL DEFAULT 0,
  completed_jobs     INTEGER      NOT NULL DEFAULT 0,
  failed_jobs        INTEGER      NOT NULL DEFAULT 0,
  dead_lettered_jobs INTEGER      NOT NULL DEFAULT 0,
  running_jobs       INTEGER      NOT NULL DEFAULT 0,
  created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  started_at         TIMESTAMPTZ,
  completed_at       TIMESTAMPTZ,
  updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
```

A batch tracks:
- `id` — unique batch identifier
- `name` — client-supplied descriptive name
- `status` — derived state (see §5)
- `total_jobs` — count of jobs originally submitted in this batch
- `completed_jobs` — jobs that reached `completed` status
- `running_jobs` — jobs currently in `running` state
- `failed_jobs` — jobs in a permanently `failed` state
- `dead_lettered_jobs` — jobs moved to the DLQ after all retries exhausted
- `created_at` / `started_at` / `completed_at` — lifecycle timestamps

#### Relationship to individual jobs
A `batch_id BIGINT REFERENCES job_batches(id) ON DELETE SET NULL` column was added to the `jobs` table. Each job submitted as part of a batch carries its batch's ID. Deleting a batch NULLifies this column on associated jobs — the individual job records themselves are never deleted.

```sql
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS batch_id BIGINT REFERENCES job_batches(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_batch_id ON jobs(batch_id);
```

#### `updateBatchProgress` — single-query aggregate UPDATE
Batch progress is recalculated using a single PostgreSQL `UPDATE … FROM (SELECT …) subquery` with conditional aggregates. No job records are loaded into Node.js:

```sql
COUNT(*) FILTER (WHERE status = 'completed')     AS completed_cnt
COUNT(*) FILTER (WHERE status = 'failed')        AS failed_cnt
COUNT(*) FILTER (WHERE status = 'dead_lettered') AS dead_lettered_cnt
COUNT(*) FILTER (WHERE status = 'running')       AS running_cnt
COUNT(*)                                         AS actual_total
```

The batch `status` column is derived from these counts in the same query and written back atomically.

### 4. Bulk Job API

```
POST /jobs/bulk
```

**Request body:**
```json
{
  "name": "Nightly Processing",
  "jobs": [
    { "task": "fetch-data",      "payload": { "source": "api" }, "priority": 5 },
    { "task": "process-data",    "payload": { "mode": "full" },  "priority": 3 },
    { "task": "generate-report", "payload": { "format": "pdf" }, "priority": 7 }
  ]
}
```

**Validation enforced:**
- `name` must be a non-empty string.
- `jobs` must be a non-empty array.
- Maximum batch size: 100 jobs.
- Each job must include a valid `task` string.
- `priority` must be within the existing 1–10 range (defaults to 5 if omitted).

**Response (201):**
```json
{
  "batchId": "10",
  "name": "Nightly Processing",
  "status": "pending",
  "totalJobs": 3,
  "jobIds": ["140", "141", "142"]
}
```

Each submitted job receives its own BullMQ job ID and its own PostgreSQL `jobs` record. Each job remains independently retryable, queryable via `GET /jobs/:id`, and processable under the full existing retry, priority, delay, and DLQ semantics.

**Idempotency:** The `Idempotency-Key` HTTP header is supported for bulk requests using the Redis namespace `idempotency:bulk:<key>`, kept separate from single-job idempotency keys. On a duplicate request, the existing batch record and job IDs are returned with `"isDuplicate": true`.

### 5. Batch Status Lifecycle

```
pending
  │
  └──► running   (first job starts or completes)
         │
         ├──► completed   (all jobs completed successfully)
         │
         └──► partial     (≥1 completed, ≥1 dead-lettered or failed)
```

| Status | Meaning |
|--------|---------|
| `pending` | Batch created; no jobs have started yet |
| `running` | At least one job has started or finished; batch is not fully terminal |
| `completed` | Every job in the batch completed successfully; `progress = 100%` |
| `partial` | At least one job permanently dead-lettered or failed while at least one other job completed; `progress = 100%` |

`progress` is calculated as:
```
progress = (completed + failed + dead_lettered) / total_jobs × 100
```

It reaches 100% when every job has reached a terminal state — whether successful or not — which is when `completed`, `partial`, or `failed` status is assigned.

Batch status is **never** trusted from client input. It is always derived from the aggregate of actual individual job states stored in PostgreSQL.

### 6. Failure & DLQ Integration

Batch processing does **not** replace or modify the existing retry and Dead Letter Queue architecture. Individual jobs inside a batch continue to use all existing mechanisms unchanged:

- **Retry attempts** with **exponential backoff** (BullMQ-managed)
- **Permanent failure** detection after all attempts exhausted
- **DLQ snapshot** creation in `orchestrix-dlq`
- **PostgreSQL status** transition to `dead_lettered`
- **Manual DLQ replay** via `POST /jobs/:id/retry`

When a batch job is manually replayed from the DLQ, the new replayed job inherits the original job's `batch_id`. This means a successful replay can eventually move a `partial` or `running` batch toward a more complete state as the replayed job's outcome is reflected in `updateBatchProgress`.

The `replayed_from_job_id` lineage is fully preserved alongside the `batch_id` association.

If one job in a batch is permanently dead-lettered while others succeed, the batch becomes `partial`. A batch only becomes `completed` when every single job reaches `completed` status.

### 7. Batch APIs

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/jobs/bulk` | Create a named batch and enqueue individual BullMQ jobs |
| `GET` | `/batches` | List all batches with summary (id, name, status, progress, timestamps) |
| `GET` | `/batches/:id` | Full batch details including all associated job records and their current statuses |
| `DELETE` | `/batches/:id` | Delete batch metadata only |

**`DELETE /batches/:id` — metadata-only deletion:**
Deleting a batch:
- Removes the `job_batches` record.
- Sets `batch_id = NULL` on all associated `jobs` rows.
- Does **not** delete individual job records.
- Does **not** remove DLQ entries.
- Does **not** modify job execution history, attempt counts, failure reasons, or timestamps.
- Does **not** remove jobs from BullMQ/Redis.

The individual job execution history is fully preserved and remains queryable via `GET /jobs/:id` after the batch is deleted.

Batch metrics are available at `GET /metrics/batches` and are also included in the master `GET /metrics` response under `summary.batches`.

### 8. Verified Testing

All verification was performed manually against a live API server and worker process.

**Successful batch (Batch 10):**
- Submitted 3 jobs: BullMQ IDs 140, 141, 142
- Task types: `fetch-data`, `process-data`, `generate-report`
- All 3 jobs picked up by workers and completed normally
- Batch transitioned: `pending → running → completed`
- Final status: `completed`, `progress: 100%`, `completedJobs: 3`

**Partial failure batch (Batch 11):**
- Submitted 3 jobs: BullMQ IDs 143, 144, 145
- Jobs 143 (`fetch-data`) and 145 (`generate-report`) completed successfully
- Job 144 (`simulate-failure`) threw a simulated error on every attempt
- BullMQ retried job 144 three times with exponential backoff
- After attempt 3, job 144 was permanently failed and a DLQ snapshot was written to `orchestrix-dlq`
- PostgreSQL status for job 144 transitioned to `dead_lettered`
- `updateBatchProgress` was called after the DLQ transition
- Final batch status: `partial`, `progress: 100%`, `completedJobs: 2`, `deadLetteredJobs: 1`

**Batch deletion verification:**
- Batch 11 was deleted via `DELETE /batches/11`
- `GET /batches/11` returned `404` confirming the batch record was removed
- Job 144 remained queryable afterward via `GET /jobs/144`
- Its failure information, `dead_lettered` status, attempt count (`3/3`), and `failedReason` were all intact

**Build result:**
```
npm run build → exit code 0, zero TypeScript errors
```

### 9. Worker Behavior

Workers require no batch-specific logic. Each worker picks up individual BullMQ jobs exactly as before using the existing execution, retry, priority, and DLQ mechanisms. The worker is only aware of batches through the `updateBatchProgress(batchId)` call inserted into the existing job lifecycle event handlers (`completed`, `failed`, `stalled`).

The `batchId` is retrieved from the `RETURNING batch_id` clause added to the existing PostgreSQL job update queries. If a job has no batch association (`batch_id IS NULL`), `updateBatchProgress` is never called — there is zero overhead for non-batch jobs.

Workers continue processing jobs as they always did. The batch layer is invisible to worker execution logic.

### 10. Architectural Tradeoff

The batch layer is deliberately an **orchestration and tracking overlay**, not a parallel execution system. This preserves all guarantees of the underlying BullMQ architecture:

- Individual jobs remain independently retryable, prioritizable, and delayable.
- Batch deletion is non-destructive to job history.
- Batch status is a read-consistent aggregate derived from PostgreSQL, not a cached or speculative value.
- The system has no new single point of failure — if the batch record is lost (e.g., batch deleted), individual jobs continue to exist and execute normally.

The tradeoff is that batch progress reflects only what PostgreSQL knows, which means there can be a brief lag between a BullMQ event (e.g., job completed in Redis) and the batch status being updated (dependent on the worker's `completed` event firing and the PostgreSQL write completing). In practice this lag is negligible (sub-second) under normal operating conditions.

---

## 33. Job Cancellation — Phase 1 (Non-Active Jobs)

### 1. Purpose
Job cancellation was added to give clients control over jobs that have been enqueued but have not yet executed. Prior to this feature, a job submitted with a long delay or sitting in a retry backoff loop could not be aborted. Upstream pipeline failures, manual user aborts, or changed application state required the ability to stop non-active jobs before worker compute resources were consumed.

### 2. Phase 1 Scope & Boundary
Phase 1 implements cancellation exclusively for **non-active jobs**:
- Jobs waiting in the BullMQ queue (`waiting` or `prioritized`)
- Jobs delayed for future execution (`delayed`)
- Jobs currently in exponential backoff waiting to retry (`retrying`)

> [!IMPORTANT]
> **Phase 2 Explicit Boundary:** Cancelling actively executing worker jobs (jobs currently running in a worker process) requires cooperative cancellation via Redis Pub/Sub and `AbortController`. **Phase 2 active-job cancellation is NOT implemented yet.** Actively running jobs are rejected with `409 Conflict`.

### 3. Database Schema Extensions
The PostgreSQL schema was extended with dedicated cancellation fields while maintaining full job history:

```sql
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMPTZ;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS cancelled_by TEXT;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS cancellation_reason TEXT;
ALTER TABLE job_batches ADD COLUMN IF NOT EXISTS cancelled_jobs INTEGER NOT NULL DEFAULT 0;
```

- `cancelled_at` — Timestamp when cancellation occurred.
- `cancelled_by` — Actor or identity that initiated cancellation (defaults to `'user'`).
- `cancellation_reason` — Optional text description of why the job was aborted.

### 4. API Endpoint (`POST /jobs/:id/cancel`)
The endpoint accepts optional JSON body parameters:
```json
{
  "reason": "Upstream pipeline cancelled by user",
  "cancelledBy": "admin-user-42"
}
```

#### Response Cases:
1. **200 OK — Successful Cancellation**: Job was in `pending` or `retrying` status (BullMQ state `waiting` or `delayed`). Removed from BullMQ Redis queue and marked `'cancelled'` in PostgreSQL.
2. **409 Conflict — Actively Running Job**: Job is currently executing in a worker process (`running` status or BullMQ state `active`). Response explicitly notes that active worker cancellation is reserved for Phase 2.
3. **400 Bad Request — Terminal Job**: Job is already in a terminal state (`completed`, `failed`, `dead_lettered`, or `cancelled`).
4. **404 Not Found — Nonexistent Job**: Job ID does not exist in PostgreSQL or BullMQ queues.

### 5. BullMQ Removal & Historical Preservation
When a non-active job is cancelled:
- `bullmqJob.remove()` purges the job from Redis (`wait` or `delayed` set), preventing any worker from picking it up.
- The PostgreSQL `jobs` row is **never deleted**. It is updated to `status = 'cancelled'`, preserving full execution history, attempt counts, prior failure reasons, and cancellation metadata for auditing.

### 6. Batch Progress & Status Handling
Batch progress calculations (`updateBatchProgress`) incorporate cancelled jobs as a terminal state:
```sql
progress = (completed_cnt + failed_cnt + dead_lettered_cnt + cancelled_cnt) / total_jobs × 100
```
- If **all** jobs in a batch are cancelled, batch status transitions to `'cancelled'`.
- If a mix of jobs complete and cancel, the batch status transitions to `'partial'` once all jobs reach a terminal state (`progress = 100%`).

### 7. Worker Pre-Execution Guard
To eliminate race conditions where a worker pops a job from Redis at the exact millisecond `POST /jobs/:id/cancel` is called:
- Right inside the worker callback, before marking the job `'running'`, the worker performs a pre-execution check:
  ```typescript
  const dbJob = await getJobByBullId(job.id!);
  if (dbJob && dbJob.status === 'cancelled') {
    console.warn(`[Worker] Job ${job.id} was cancelled before execution. Skipping.`);
    return { status: 'cancelled_before_execution' };
  }
  ```
- If the job was already cancelled in PostgreSQL, the worker cleanly aborts execution without processing task logic.

### 8. Metrics Integration
System metrics endpoints (`GET /metrics` and `GET /metrics/batches`) integrate cancellation statistics:
- `cancelledJobs` included in job counts and success/failure rate denominators.
- `cancelledBatches` reported under batch summary metrics.

### 9. Manual Verification & Test Results

All Phase 1 behavior was manually verified against a live Express API server and worker process:

- **Job 148 (Delayed Job Cancellation)**:
  - Enqueued with `delay: 60000` (BullMQ state `delayed`).
  - Called `POST /jobs/148/cancel`.
  - Returned **200 OK**. Job removed from Redis delayed set; PostgreSQL status updated to `'cancelled'` with `cancelled_at` timestamp.
- **Job 151 (Active Test Race Analysis)**:
  - Submitted test job to verify `409 Conflict`.
  - `GET /jobs/151` initially showed `state: active`.
  - Subsequent `POST /jobs/151/cancel` returned **400 Bad Request** (`already in terminal state 'completed'`).
  - **Investigation Result**: Exposed a testing race condition. The payload used `{ "taskType": "simulate-long-job" }` instead of `{ "task": "simulate-long-job" }`. Because the payload key did not match `job.data.task`, the worker fell back to `await delay(3000)` (3-second default) instead of 60 seconds. The job completed in 3 seconds before the cancel request arrived.
- **Job 152 (Active Job Conflict Verification)**:
  - Correctly submitted with `{ "task": "simulate-long-job" }` (60-second worker delay).
  - Verified `GET /jobs/152` returned `state: active`, status `'running'`.
  - Called `POST /jobs/152/cancel`.
  - Returned **409 Conflict**: `"Job 152 is actively being processed by a worker. Active worker cancellation requires Phase 2."`
- **Terminal Job Rejection**:
  - Called `POST /jobs/:id/cancel` on a completed job.
  - Returned **400 Bad Request**: `"Cannot cancel job because it is already in terminal state 'completed'"`.

### 10. Phase 2 Status Statement
**Active-job cancellation is implemented in Phase 2 (Section 34).** The feature uses Redis Pub/Sub cancellation signals and cooperative `AbortController` cancellation coordinated between the API server and worker process.

---

## 34. Job Cancellation — Phase 2 (Active Jobs)

### 1. Why Active Cancellation Is More Complex

Phase 1 cancellation (Section 33) targets jobs in BullMQ's `waiting`, `delayed`, or `prioritized` states. These jobs are idle in Redis and have not been claimed by any worker. Cancelling them is straightforward: remove the BullMQ job entry and update PostgreSQL atomically.

Active cancellation is fundamentally different for four reasons:

1. **Ownership**: An active job is locked by a specific worker process. No external process can forcibly terminate the task function mid-execution — it must cooperate voluntarily.
2. **Concurrency**: The task may complete at any moment while a cancellation request is being processed. Two transitions can race: the normal completion path and the cancellation path. Only one must win.
3. **No Immediate Acknowledgement**: Sending a cancellation signal (e.g., `AbortController.abort()`) only *requests* interruption. It does not guarantee that asynchronous task code has actually stopped. A separate confirmation — the STOPPED acknowledgement — is required to guarantee termination.
4. **State Consistency**: If the cancellation request arrives while the task is committing its result to PostgreSQL, any stale update from a prior lifecycle path must be blocked to prevent a race-overwrite of the `'cancelled'` status.

---

### 2. Redis Pub/Sub Request and Acknowledgement Flow

Phase 2 uses two dedicated Redis channels:

| Channel | Direction | Purpose |
|---|---|---|
| `channel:job:cancel:req` | API → all Workers | Broadcast cancellation request |
| `channel:job:cancel:resp:<requestId>` | Worker → API | Per-request STOPPED or RACE_LOST ACK |

**Full flow (happy path — API waits for STOPPED):**

```
API Process                                  Worker Process
──────────                                   ──────────────
1. Acquire single-flight lock (NX EX 30)
2. Subscribe to channel:job:cancel:resp:<requestId>
3. Publish to channel:job:cancel:req  ─────────────────────────▶
                                             4. Receive message
                                             5. Find active context for jobId
                                             6. CAS: executing → cancelling
                                             7. Publish RECEIVED ACK ◀─────────────
                                             8. Abort AbortController
                                             9. Cooperative task throws JobCancelledError
                                            10. Write durable marker to Redis
                                            11. cancelJobInDb (WHERE status IN ('pending','retrying','running'))
                                            12. Publish STOPPED ACK ◀─────────────
4. (API waiting on ackPromise)
5. Receive STOPPED
6. Return HTTP 200
```

The `requestId` (`req_cancel_<jobId>_<timestamp>_<uuid>`) is included in every message and used as the reply-channel suffix, ensuring each cancellation request has a private acknowledgement channel that cannot cross-contaminate concurrent requests for different jobs.

---

### 3. Cooperative Cancellation Using AbortController and cancellableDelay

Each active job gets its own `AbortController` instance, created inside the worker processor callback at the start of every execution attempt:

```typescript
const abortController = new AbortController();
const context: ActiveJobContext = {
  job,
  abortController,
  state: 'executing',
  executionId: `${job.id}:${attemptNum}:${job.token || ''}`,
};
activeJobs.set(String(job.id!), context);
```

Long-running waits use `cancellableDelay` instead of `await delay(...)`:

```typescript
export function cancellableDelay(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(new JobCancelledError('Job cancelled by user'));
    const onAbort = () => { cleanup(); reject(new JobCancelledError('Job cancelled by user')); };
    const timer = setTimeout(() => { cleanup(); resolve(); }, ms);
    const cleanup = () => { clearTimeout(timer); signal?.removeEventListener('abort', onAbort); };
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}
```

When the cancellation subscriber triggers `abortController.abort()`, any `cancellableDelay` currently awaiting on that signal immediately rejects with `JobCancelledError`, unwinding the task call-stack cooperatively.

**Cancellation boundary**: Only code that awaits `cancellableDelay` is guaranteed to abort promptly. Non-abortable synchronous CPU work (e.g., pure computation, JSON parsing, crypto) runs to its next `await` before detecting abort. The implementation makes no stronger guarantee than this.

---

### 4. ActiveJobContext Lifecycle States and Race Handling

```typescript
export type ActiveJobState = 'executing' | 'completing' | 'cancelling' | 'settled';
```

| State | Meaning |
|---|---|
| `executing` | Task is running normally; eligible for cancellation |
| `completing` | Task finished work and is committing completion to PostgreSQL |
| `cancelling` | Cancellation claimed; AbortController has been signalled |
| `settled` | Final state; removed from `activeJobs` map |

All transitions go through a compare-and-swap helper that is **not atomic at the OS level**, but is safe in single-threaded Node.js because JavaScript is single-threaded and all state transitions happen synchronously between `await` points:

```typescript
export function tryTransitionJobState(
  context: ActiveJobContext,
  expectedState: ActiveJobState,
  newState: ActiveJobState
): boolean {
  if (context.state === expectedState) {
    context.state = newState;
    return true;
  }
  return false;
}
```

**Race resolution — two paths, one winner:**

- **Normal completion wins**: After task execution ends, the processor calls `tryTransitionJobState(context, 'executing', 'completing')`. If this succeeds, the completion path owns the job. Any subsequent cancellation request sees state `'completing'` and publishes `RACE_LOST`.
- **Cancellation wins**: The cancel subscriber calls `tryTransitionJobState(context, 'executing', 'cancelling')`. If this succeeds, the cancellation path owns the job. When the completion path later tries `tryTransitionJobState(context, 'executing', 'completing')`, it fails (state is already `'cancelling'`), and the processor throws `JobCancelledError` instead of writing completion.

The `updateJobCompleted` PostgreSQL query has a conditional guard `WHERE status = 'running'`. If cancellation already wrote `cancelled` to the DB, the completion `UPDATE` affects 0 rows. The worker handles this case (rowCount === 0) by checking the actual DB status and, if `cancelled`, throwing `UnrecoverableError` to prevent BullMQ retries.

---

### 5. PostgreSQL Conditional Status Updates and Durable Cancellation Markers

#### Conditional Transitions

All PostgreSQL status updates use `WHERE` clauses that enumerate only the valid predecessor states:

| Function | Guard Clause |
|---|---|
| `updateJobStarted` | `AND status IN ('pending', 'retrying')` |
| `updateJobCompleted` | `AND status = 'running'` |
| `updateJobFailed` | `AND status = 'running'` |
| `cancelJobInDb` | `AND status IN ('pending', 'retrying', 'running')` |

This ensures no status can overwrite a job that has already reached a terminal state (`completed`, `cancelled`, `failed`, `dead_lettered`). For example, if cancellation commits first (setting status to `'cancelled'`), a subsequent completion `UPDATE` with `AND status = 'running'` returns 0 rows and is silently ignored.

#### Durable Cancellation Marker

Before calling `cancelJobInDb`, the worker writes a durable Redis marker:

```
key:   job:cancel:durable:<jobId>
value: { jobId, executionId, cancelledBy, reason, stoppedAt, workerPid }
TTL:   none (explicit delete on success)
```

This marker serves as a crash-recovery record. If the worker writes cancellation intent to Redis but crashes before PostgreSQL is updated, the marker persists. The next `GET /jobs/:id` request triggers `reconcileJobStatus`, which:

1. Detects the durable marker.
2. Acquires a Redis lock to prevent concurrent reconciliation.
3. Calls `cancelJobInDb` on behalf of the crashed worker.
4. If successful (rowCount === 1), deletes the marker and updates batch progress.
5. If the job is already in a terminal state, deletes the marker.
6. If the outcome is ambiguous (e.g., PostgreSQL error), retains the marker for the next reconciliation attempt.

---

### 6. API Response Codes

| Condition | HTTP Status | Response |
|---|---|---|
| Worker confirmed STOPPED; PostgreSQL updated to `'cancelled'` | **200 OK** | `{ message: 'Active job cancelled successfully', status: 'cancelled' }` |
| Cancellation arrived after job reached `'completing'` state | **409 Conflict** | `{ error: 'Conflict', message: 'Job completed naturally before cancellation could stop it' }` |
| No STOPPED or RACE_LOST ACK received within 5000ms | **504 Gateway Timeout** | `{ error: 'Gateway Timeout', message: 'Worker acknowledged cancellation request, but execution stoppage could not be confirmed within 5000ms' }` |
| Another cancellation request already in progress (single-flight lock held) | **409 Conflict** | `{ error: 'Conflict', message: 'Cancellation already in progress for job <id>' }` |
| Redis Pub/Sub error | **503 Service Unavailable** | `{ error: 'Service Unavailable', message: 'Failed to communicate cancellation request via Redis' }` |
| Job already in a terminal state | **400 Bad Request** | `{ error: 'Bad Request', message: 'Cannot cancel job ... already in terminal state ...' }` |
| Job not found | **404 Not Found** | `{ error: 'Job with ID ... not found' }` |

The single-flight lock (`job:cancel:in_flight:<jobId>`, NX EX 30s) prevents duplicate concurrent cancellation requests for the same job from simultaneously subscribing and waiting. The lock is released atomically using a Lua compare-and-delete script in `releaseCancelLock` so that only the request that originally acquired the lock can release it.

---

### 7. Avoiding Retries, Failure Updates, and DLQ Insertion

When the worker processor cancellation path exits, it throws `UnrecoverableError('Job cancelled by user')`. This has two effects:

1. **BullMQ**: `UnrecoverableError` bypasses BullMQ's retry scheduling. BullMQ marks the job `failed` internally in Redis but does not schedule another attempt.
2. **`worker.on('failed')` guard**: The failed event handler checks whether the error is cancellation-related before doing anything:

```typescript
worker.on('failed', async (job, err) => {
  if (
    err?.name === 'JobCancelledError' ||
    err?.message?.includes('Job cancelled') ||
    (err instanceof UnrecoverableError && err.message.includes('cancelled'))
  ) {
    console.log(`[Worker] Job ${job?.id} cancelled. Skipping retries, failure update, and DLQ.`);
    return;  // ← exits immediately; no updateJobFailed, no dlqQueue.add
  }
  // ... normal failure and DLQ handling below ...
});
```

A cancelled job is therefore **never** written to the Dead Letter Queue (`orchestrix-dlq`), and PostgreSQL is **never** updated to `'failed'` or `'dead_lettered'` for a cancelled job. The PostgreSQL status remains `'cancelled'`, set by `cancelJobInDb`.

---

### 8. GET /jobs/:id State Normalization

BullMQ does not have a `cancelled` job state. When the worker throws `UnrecoverableError`, BullMQ records the job in its own `failed` state in Redis — the same state it uses for any permanently-failed job. This creates a discrepancy: BullMQ reports `'failed'`, but the semantic meaning is user cancellation.

The API resolves this by checking the PostgreSQL `status` column **before** calling BullMQ's `job.getState()`:

```typescript
// src/api.ts — GET /jobs/:id state calculation
const state =
  dbJob?.status === 'cancelled'
    ? 'cancelled'
    : (job ? await job.getState() : (dbJob?.status || 'unknown'));
```

**Rule**: When PostgreSQL `status = 'cancelled'`, the API returns `state: 'cancelled'` and never calls `job.getState()`. Clients always receive the semantically correct state. BullMQ's internal `failed` record in Redis is not exposed.

---

### 9. Durable Reconciliation and Idempotency

The reconciliation function (`reconcileJobStatus` in `src/api.ts`) handles the following edge cases:

| Scenario | Reconciliation Behaviour |
|---|---|
| Worker wrote durable marker then crashed | Next `GET /jobs/:id` triggers `cancelJobInDb`; idempotent if job is already `'cancelled'` |
| Marker exists but job already completed | `cancelJobInDb` returns rowCount 0 (guard clause blocks overwrite); marker is deleted |
| Marker exists but job is `dead_lettered` | Same as above; marker is deleted |
| PostgreSQL is temporarily unavailable | Error is caught and logged; marker is **retained** for next attempt |
| Two concurrent `GET /jobs/:id` calls | Redis lock (`job:cancel:reconcile:<jobId>`, NX EX 10s) prevents duplicate reconciliation |

The `executionId` stored in the durable marker (`<jobId>:<attemptNum>:<token>`) allows future reconciliation logic to distinguish between a marker written by an earlier execution attempt and a current execution. If the job has been retried (new `executionId` in BullMQ), reconciliation from a stale prior-execution marker will not falsely cancel the new execution.

---

### 10. Automated Test Script

The test script at `scripts/test-active-cancellation.js` automates the full Phase 2 cancellation verification flow. It uses only Node.js built-in `http` — no external dependencies.

**Steps executed by the script:**
1. `POST /jobs` with `{ "task": "simulate-long-job" }` — submits a 60-second job.
2. `GET /jobs/:id` polled every 200 ms (up to 10 s) — waits for `state: 'active'` or `dbStatus: 'running'`.
3. `POST /jobs/:id/cancel` with `{ "reason": "...", "cancelledBy": "test_admin" }` — triggers active cancellation.
4. `GET /jobs/:id` polled every 200 ms (up to 5 s) — verifies `dbStatus: 'cancelled'`.
5. Exit code 0 (PASS) if cancellation returned HTTP 200 and `dbStatus` is `'cancelled'`; exit code 1 otherwise.

**DLQ verification is not automated in the script.** It requires two additional manual `curl` commands after the script completes (see §11).

---

### 11. Verified Test Results — Job 157 (Definitive DLQ Test)

All Phase 2 behaviour — including dedicated DLQ non-insertion — was verified against a live Express API server and worker process.

**Test tool**: `scripts/test-active-cancellation.js` + manual `curl` commands.

#### Step-by-step results

| Step | Action | Expected | Observed |
|---|---|---|---|
| 1 | `POST /jobs` → `simulate-long-job` | HTTP 201, `jobId` returned | ✅ HTTP 201, `jobId: 157` |
| 2 | Poll `GET /jobs/157` until active | `state: 'active'` or `dbStatus: 'running'` | ✅ `state: 'active'`, `dbStatus: 'running'` |
| 3 | `POST /jobs/157/cancel` | HTTP 200 | ✅ HTTP 200 |
| 4 | Poll `GET /jobs/157` for `cancelled` | `dbStatus: 'cancelled'` within 5 s | ✅ `{ state: 'cancelled', dbStatus: 'cancelled' }` |
| 5 | `GET /dlq/157` | HTTP 404 (no snapshot) | ✅ HTTP 404, `"DLQ job with ID 157 not found"` |
| 6 | `GET /dlq` — inspect full list | No entry with `originalJobId: '157'` | ✅ No such entry in `dlqJobs` array |
| 7 | Existing DLQ entries for prior failed jobs | Unchanged | ✅ Prior DLQ snapshots intact |

#### What these results confirm

- **Active before cancellation**: `GET /jobs/157` reported `state: 'active'` before the cancel request was sent. The job was genuinely running inside the worker, not merely waiting or delayed.
- **HTTP 200 confirmed**: The API waited for and received the worker's STOPPED acknowledgement before returning. The response was not a guess — it reflects a confirmed coordinated stop.
- **PostgreSQL status `cancelled`**: `cancelJobInDb` executed atomically with `WHERE status IN ('pending','retrying','running')`, committing the final status.
- **BullMQ internal state is `failed`**: BullMQ has no `cancelled` state. Internally, the job is stored as `failed` in Redis because the processor threw `UnrecoverableError`. The API normalises this — `GET /jobs/157` returns `state: 'cancelled'`, not `state: 'failed'`. This is by design.
- **No DLQ snapshot created**: `GET /dlq/157` returned HTTP 404. `GET /dlq` confirmed the `dlqJobs` array contained no entry with `originalJobId: '157'`. The `worker.on('failed')` guard returned immediately before reaching `dlqQueue.add()`.
- **Existing DLQ snapshots preserved**: Snapshots from prior permanently-failed jobs (e.g. from earlier `simulate-failure` tests) remained in `GET /dlq`, confirming the guard is selective and does not affect unrelated entries.

#### Test commands (Windows CMD)

```cmd
rem Run automated script (note the Job ID printed in output)
node scripts\test-active-cancellation.js

rem Verify no DLQ snapshot for the specific job (replace 157 with actual ID)
curl -s http://localhost:3000/dlq/157

rem Verify full DLQ list contains no reference to the job
curl -s http://localhost:3000/dlq
```

---

### 12. Architectural Summary

```
POST /jobs/:id/cancel (active job)
  │
  ├─ Acquire single-flight lock (NX EX 30)
  ├─ Subscribe to channel:job:cancel:resp:<requestId>
  ├─ Publish to channel:job:cancel:req
  │        │
  │        └─ Worker receives → CAS(executing→cancelling)
  │                          → Publish RECEIVED ACK
  │                          → abort() AbortController
  │                          → cancellableDelay rejects → JobCancelledError
  │                          → Write durable Redis marker
  │                          → cancelJobInDb (WHERE status IN (...))
  │                          → Publish STOPPED ACK
  │
  ├─ API receives STOPPED
  ├─ Release single-flight lock (Lua compare-and-delete)
  └─ HTTP 200

worker.on('failed') → cancellation guard returns early → no updateJobFailed, no DLQ
GET /jobs/:id       → dbStatus='cancelled' → API returns state='cancelled' (not BullMQ 'failed')
GET /jobs/:id       → durable marker found → reconcileJobStatus triggers cancelJobInDb
GET /dlq/:id        → HTTP 404 for cancelled jobs (no snapshot was ever written)
```

