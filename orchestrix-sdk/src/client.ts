import {
  OrchestrixError,
  OrchestrixValidationError,
  OrchestrixApiError,
  OrchestrixNotFoundError,
  OrchestrixTimeoutError,
  OrchestrixConnectionError,
} from './errors';
import {
  OrchestrixClientOptions,
  CreateJobOptions,
  CreateJobResponse,
  JobDetails,
  GetJobOptions,
} from './types';

const DEFAULT_TIMEOUT_MS = 10000;

export class JobsResource {
  private readonly client: OrchestrixClient;

  constructor(client: OrchestrixClient) {
    this.client = client;
  }

  /**
   * Enqueues a new job on the Orchestrix server.
   *
   * @param data Job payload object containing job parameter data.
   * @param options Optional priority, delay, idempotencyKey, and per-request timeoutMs.
   * @returns Promise resolving to the created job response.
   */
  async create(
    data: Record<string, any>,
    options: CreateJobOptions = {}
  ): Promise<CreateJobResponse> {
    if (!data || typeof data !== 'object' || Array.isArray(data)) {
      throw new OrchestrixValidationError('Job data must be a valid non-null object');
    }

    if (options.priority !== undefined) {
      if (!Number.isInteger(options.priority) || options.priority < 1 || options.priority > 10) {
        throw new OrchestrixValidationError(
          `Invalid priority "${options.priority}". Priority must be an integer between 1 and 10.`
        );
      }
    }

    if (options.delay !== undefined) {
      if (typeof options.delay !== 'number' || options.delay < 0 || isNaN(options.delay)) {
        throw new OrchestrixValidationError(
          `Invalid delay "${options.delay}". Delay must be a non-negative number in milliseconds.`
        );
      }
    }

    const payload: Record<string, any> = { ...data };
    if (options.priority !== undefined) {
      payload.priority = options.priority;
    }
    if (options.delay !== undefined) {
      payload.delay = options.delay;
    }

    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    };

    if (options.idempotencyKey && options.idempotencyKey.trim().length > 0) {
      headers['Idempotency-Key'] = options.idempotencyKey.trim();
    }

    return this.client.request<CreateJobResponse>('/jobs', {
      method: 'POST',
      headers,
      body: JSON.stringify(payload),
      timeoutMs: options.timeoutMs,
    });
  }

  /**
   * Retrieves status and details of a job by ID.
   *
   * @param jobId Unique ID of the job to fetch.
   * @param options Optional per-request options (e.g. timeoutMs).
   * @returns Promise resolving to JobDetails object.
   */
  async get(jobId: string, options: GetJobOptions = {}): Promise<JobDetails> {
    if (!jobId || typeof jobId !== 'string' || jobId.trim().length === 0) {
      throw new OrchestrixValidationError('jobId must be a non-empty string');
    }

    const sanitizedId = jobId.trim();
    return this.client.request<JobDetails>(`/jobs/${encodeURIComponent(sanitizedId)}`, {
      method: 'GET',
      timeoutMs: options.timeoutMs,
    });
  }
}

export class OrchestrixClient {
  public readonly baseUrl: string;
  public readonly timeoutMs: number;
  public readonly jobs: JobsResource;

  constructor(options: string | OrchestrixClientOptions) {
    let rawBaseUrl: string;
    let timeout: number = DEFAULT_TIMEOUT_MS;

    if (typeof options === 'string') {
      rawBaseUrl = options;
    } else if (options && typeof options === 'object') {
      rawBaseUrl = options.baseUrl;
      if (options.timeoutMs !== undefined) {
        if (typeof options.timeoutMs !== 'number' || options.timeoutMs <= 0 || isNaN(options.timeoutMs)) {
          throw new OrchestrixValidationError(
            `Invalid timeoutMs "${options.timeoutMs}". Must be a positive number.`
          );
        }
        timeout = options.timeoutMs;
      }
    } else {
      throw new OrchestrixValidationError(
        'OrchestrixClient requires a baseUrl string or configuration object containing baseUrl'
      );
    }

    if (!rawBaseUrl || typeof rawBaseUrl !== 'string' || rawBaseUrl.trim().length === 0) {
      throw new OrchestrixValidationError('baseUrl must be a non-empty string');
    }

    try {
      const parsedUrl = new URL(rawBaseUrl.trim());
      if (parsedUrl.protocol !== 'http:' && parsedUrl.protocol !== 'https:') {
        throw new OrchestrixValidationError(
          `Invalid protocol "${parsedUrl.protocol}" in baseUrl. Must be http or https.`
        );
      }
    } catch (err: any) {
      if (err instanceof OrchestrixValidationError) {
        throw err;
      }
      throw new OrchestrixValidationError(`Invalid baseUrl URL string "${rawBaseUrl}": ${err.message}`);
    }

    // Strip trailing slashes for predictable path concatenation
    this.baseUrl = rawBaseUrl.trim().replace(/\/+$/, '');
    this.timeoutMs = timeout;
    this.jobs = new JobsResource(this);
  }

  /**
   * Internal helper method to execute HTTP requests with timeout, error handling, and type parsing.
   */
  async request<T>(
    path: string,
    init: {
      method: 'GET' | 'POST';
      headers?: Record<string, string>;
      body?: string;
      timeoutMs?: number;
    }
  ): Promise<T> {
    const url = `${this.baseUrl}${path}`;
    const effectiveTimeout = init.timeoutMs ?? this.timeoutMs;

    const controller = new AbortController();
    let isTimeoutTriggered = false;

    const timeoutId = setTimeout(() => {
      isTimeoutTriggered = true;
      controller.abort();
    }, effectiveTimeout);

    let res: Response;
    try {
      res = await fetch(url, {
        method: init.method,
        headers: init.headers,
        body: init.body,
        signal: controller.signal,
      });
    } catch (err: any) {
      if (isTimeoutTriggered || controller.signal.aborted) {
        throw new OrchestrixTimeoutError(
          `Request to ${url} timed out after ${effectiveTimeout}ms`,
          effectiveTimeout
        );
      }
      throw new OrchestrixConnectionError(
        `Failed to connect to Orchestrix server at ${url}: ${err?.message || err}`,
        err instanceof Error ? err : undefined
      );
    } finally {
      clearTimeout(timeoutId);
    }

    let responseData: any;
    const contentType = res.headers.get('content-type') || '';
    if (contentType.includes('application/json')) {
      try {
        responseData = await res.json();
      } catch (jsonErr) {
        responseData = null;
      }
    } else {
      try {
        responseData = await res.text();
      } catch (textErr) {
        responseData = null;
      }
    }

    if (!res.ok) {
      if (res.status === 404) {
        throw new OrchestrixNotFoundError(res.statusText, responseData);
      }
      throw new OrchestrixApiError(res.status, res.statusText, responseData);
    }

    return responseData as T;
  }
}
