/**
 * Base class for all Orchestrix SDK errors.
 */
export class OrchestrixError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'OrchestrixError';
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * Thrown when client-side input validation fails (e.g. invalid baseUrl, empty jobId).
 */
export class OrchestrixValidationError extends OrchestrixError {
  constructor(message: string) {
    super(message);
    this.name = 'OrchestrixValidationError';
  }
}

/**
 * Thrown when a request to the Orchestrix API returns an HTTP error status (4xx or 5xx).
 */
export class OrchestrixApiError extends OrchestrixError {
  public readonly status: number;
  public readonly statusText: string;
  public readonly body: any;

  constructor(status: number, statusText: string, body: any, message?: string) {
    const errorMsg =
      message ||
      (typeof body === 'object' && body?.error ? body.error : `HTTP ${status} ${statusText}`);
    super(errorMsg);
    this.name = 'OrchestrixApiError';
    this.status = status;
    this.statusText = statusText;
    this.body = body;
  }
}

/**
 * Specific error thrown when a requested resource (e.g. job) is not found (HTTP 404).
 */
export class OrchestrixNotFoundError extends OrchestrixApiError {
  constructor(statusText: string, body: any, message?: string) {
    super(404, statusText, body, message);
    this.name = 'OrchestrixNotFoundError';
  }
}

/**
 * Thrown when an HTTP request exceeds the specified timeout duration.
 */
export class OrchestrixTimeoutError extends OrchestrixError {
  public readonly timeoutMs: number;

  constructor(message: string, timeoutMs: number) {
    super(message);
    this.name = 'OrchestrixTimeoutError';
    this.timeoutMs = timeoutMs;
  }
}

/**
 * Thrown when a network connection failure occurs (e.g. server unreachable, connection refused, DNS failure).
 */
export class OrchestrixConnectionError extends OrchestrixError {
  public readonly cause?: Error;

  constructor(message: string, cause?: Error) {
    super(message);
    this.name = 'OrchestrixConnectionError';
    this.cause = cause;
  }
}
