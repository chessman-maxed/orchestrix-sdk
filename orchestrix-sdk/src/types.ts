/**
 * Options for configuring the OrchestrixClient.
 */
export interface OrchestrixClientOptions {
  /**
   * Base URL of the Orchestrix API server (e.g. "http://localhost:3000").
   */
  baseUrl: string;

  /**
   * Request timeout in milliseconds. Defaults to 10000 ms (10 seconds).
   */
  timeoutMs?: number;
}

/**
 * Options for creating a new job via jobs.create().
 */
export interface CreateJobOptions {
  /**
   * Job priority from 1 (lowest) to 10 (highest). Defaults to 5.
   */
  priority?: number;

  /**
   * Execution delay in milliseconds. Defaults to 0 (immediate execution).
   */
  delay?: number;

  /**
   * Optional Idempotency-Key header for request deduplication.
   */
  idempotencyKey?: string;

  /**
   * Request timeout override in milliseconds for this specific call.
   */
  timeoutMs?: number;
}

/**
 * Response payload returned after creating a job via POST /jobs.
 */
export interface CreateJobResponse {
  /**
   * Human-readable status message from the server.
   */
  message: string;

  /**
   * Unique ID assigned to the job.
   */
  jobId: string;

  /**
   * Name/type of the enqueued task (e.g. 'default-job').
   */
  name: string;

  /**
   * Effective job priority (1-10).
   */
  priority: number;

  /**
   * Delay in milliseconds.
   */
  delay: number;

  /**
   * Flag indicating whether an existing duplicate job was returned due to idempotency key match.
   */
  isDuplicate: boolean;

  /**
   * The job payload data object.
   */
  data: Record<string, any>;

  /**
   * Job state (present when returned via idempotency duplicate check).
   */
  state?: string;

  /**
   * Number of execution attempts made so far.
   */
  attemptsMade?: number;

  /**
   * Maximum allowed execution attempts.
   */
  maxAttempts?: number;

  /**
   * Progress indicator percentage or numeric value.
   */
  progress?: number;

  /**
   * Error message if the job has failed.
   */
  failedReason?: string | null;

  /**
   * Return value produced by worker execution.
   */
  returnvalue?: any;

  /**
   * Timestamp (epoch ms) when the job was created.
   */
  timestamp?: number;

  /**
   * Timestamp (epoch ms) when job finished execution, or null if incomplete.
   */
  finishedOn?: number | null;
}

/**
 * Detailed job information returned by jobs.get().
 */
export interface JobDetails {
  /**
   * Unique job ID.
   */
  id: string;

  /**
   * Current BullMQ queue state ('waiting', 'active', 'completed', 'failed', 'delayed', 'cancelled', 'unknown').
   */
  state: string;

  /**
   * Database status recorded in PostgreSQL, or null.
   */
  dbStatus: string | null;

  /**
   * Job priority (1-10).
   */
  priority: number;

  /**
   * Delay in milliseconds.
   */
  delay: number;

  /**
   * Number of execution attempts made.
   */
  attemptsMade: number;

  /**
   * Maximum allowed attempts.
   */
  maxAttempts: number;

  /**
   * Job payload data object.
   */
  data: Record<string, any> | null;

  /**
   * Progress percentage or value.
   */
  progress: number;

  /**
   * Failure reason if job failed, otherwise null.
   */
  failedReason: string | null;

  /**
   * ISO string timestamp when job was cancelled, or null.
   */
  cancelledAt: string | null;

  /**
   * Entity or user that initiated cancellation, or null.
   */
  cancelledBy: string | null;

  /**
   * Cancellation reason description, or null.
   */
  cancellationReason: string | null;

  /**
   * Value returned by worker task function upon successful completion.
   */
  returnvalue: any;

  /**
   * Epoch millisecond timestamp of job creation, or null.
   */
  timestamp: number | null;

  /**
   * Epoch millisecond timestamp of job completion, or null.
   */
  finishedOn: number | null;
}

/**
 * Options for jobs.get() method call.
 */
export interface GetJobOptions {
  /**
   * Request timeout override in milliseconds for this specific call.
   */
  timeoutMs?: number;
}
