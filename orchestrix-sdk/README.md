# Orchestrix SDK (`orchestrix-sdk`)

Official Node.js / TypeScript SDK for the **Orchestrix** job orchestration and scheduling engine.

> ⚠️ **Note:** The SDK connects to an existing Orchestrix backend server. Installing this SDK does not automatically start or run the Orchestrix backend server.

---

## Prerequisites

- **Node.js**: Version `>= 18.0.0` (uses native `fetch` and `AbortController`)
- **Orchestrix Backend**: A running Orchestrix API server instance (e.g. `http://localhost:3000`)

---

## Installation

```bash
npm install orchestrix-sdk
```

---

## Quickstart

### 1. Initialize the Client

You can initialize `OrchestrixClient` using a URL string or an options object.

```typescript
import { OrchestrixClient } from 'orchestrix-sdk';

// Using a configuration object
const client = new OrchestrixClient({
  baseUrl: 'http://localhost:3000',
  timeoutMs: 10000, // Optional per-client timeout (default: 10000ms)
});

// Or shorthand with just the base URL:
// const client = new OrchestrixClient('http://localhost:3000');
```

---

### 2. Create a Job (`client.jobs.create`)

Enqueue a new job payload with optional priority, execution delay, and idempotency key.

```typescript
try {
  const result = await client.jobs.create(
    {
      task: 'process-report',
      reportId: 42,
      recipient: 'user@example.com',
    },
    {
      priority: 5,        // Priority from 1 (lowest) to 10 (highest)
      delay: 5000,        // Execution delay in milliseconds
      idempotencyKey: 'report-42-unique-key', // Optional header for deduplication
    }
  );

  console.log(`Job Enqueued Successfully!`);
  console.log(`Job ID: ${result.jobId}`);
  console.log(`Is Duplicate: ${result.isDuplicate}`);
} catch (error) {
  console.error(`Failed to create job:`, error);
}
```

---

### 3. Retrieve Job Details (`client.jobs.get`)

Fetch current state and status details for a job using its `jobId`.

```typescript
try {
  const job = await client.jobs.get('12');

  console.log(`Job ID: ${job.id}`);
  console.log(`State: ${job.state}`);             // 'waiting', 'active', 'completed', 'failed', etc.
  console.log(`DB Status: ${job.dbStatus}`);       // Database status recorded in PostgreSQL
  console.log(`Progress: ${job.progress}%`);
  console.log(`Attempts Made: ${job.attemptsMade} / ${job.maxAttempts}`);
  console.log(`Payload Data:`, job.data);
} catch (error) {
  console.error(`Failed to retrieve job:`, error);
}
```

---

## Error Handling

The SDK exports specific error classes for granular error catching:

| Error Class | Trigger Description |
| :--- | :--- |
| `OrchestrixError` | Base class for all SDK errors |
| `OrchestrixValidationError` | Thrown when client-side parameter validation fails (e.g. invalid `priority` or empty `jobId`) |
| `OrchestrixApiError` | Thrown when server returns a non-2xx HTTP status code (e.g., 500 Internal Error) |
| `OrchestrixNotFoundError` | Subclass of `OrchestrixApiError` thrown when job ID is not found (HTTP 404) |
| `OrchestrixTimeoutError` | Thrown when an HTTP request exceeds `timeoutMs` |
| `OrchestrixConnectionError` | Thrown when network connection to the server fails (e.g., server offline) |

### Example Error Handling

```typescript
import {
  OrchestrixClient,
  OrchestrixNotFoundError,
  OrchestrixValidationError,
  OrchestrixConnectionError,
  OrchestrixTimeoutError,
} from 'orchestrix-sdk';

const client = new OrchestrixClient('http://localhost:3000');

try {
  const job = await client.jobs.get('invalid-job-id');
} catch (err) {
  if (err instanceof OrchestrixNotFoundError) {
    console.error(`Job not found (404):`, err.message);
  } else if (err instanceof OrchestrixValidationError) {
    console.error(`Invalid client input:`, err.message);
  } else if (err instanceof OrchestrixConnectionError) {
    console.error(`Unable to connect to Orchestrix server:`, err.message);
  } else if (err instanceof OrchestrixTimeoutError) {
    console.error(`Request timed out after ${err.timeoutMs}ms`);
  } else {
    console.error(`Unexpected error:`, err);
  }
}
```

---

## Configuration Options & Defaults

### `OrchestrixClientOptions`
- `baseUrl` *(required, string)*: Base URL of the API server (e.g., `'http://localhost:3000'`). Trailing slashes are automatically sanitized.
- `timeoutMs` *(optional, number)*: Global HTTP request timeout in milliseconds. Default: `10000` (10 seconds).

### `CreateJobOptions`
- `priority` *(optional, number)*: Integer between `1` (lowest) and `10` (highest).
- `delay` *(optional, number)*: Non-negative delay in milliseconds before execution starts. Default: `0`.
- `idempotencyKey` *(optional, string)*: Value passed in `Idempotency-Key` header for request deduplication.
- `timeoutMs` *(optional, number)*: Override timeout for this specific API request.

### `GetJobOptions`
- `timeoutMs` *(optional, number)*: Override timeout for this specific API request.

---

## Current SDK Scope & Limitations

This version (`0.1.0`) supports:
- ✅ Orchestrix client configuration and request timeout management
- ✅ Job creation (`client.jobs.create`)
- ✅ Job state and progress retrieval (`client.jobs.get`)

*Job cancellation, retry management, batch operations, and worker execution handlers are reserved for future SDK releases.*
