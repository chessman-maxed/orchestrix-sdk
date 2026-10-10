/**
 * Base class for all Orchestrix SDK errors.
 */
export declare class OrchestrixError extends Error {
    constructor(message: string);
}
/**
 * Thrown when client-side input validation fails (e.g. invalid baseUrl, empty jobId).
 */
export declare class OrchestrixValidationError extends OrchestrixError {
    constructor(message: string);
}
/**
 * Thrown when a request to the Orchestrix API returns an HTTP error status (4xx or 5xx).
 */
export declare class OrchestrixApiError extends OrchestrixError {
    readonly status: number;
    readonly statusText: string;
    readonly body: any;
    constructor(status: number, statusText: string, body: any, message?: string);
}
/**
 * Specific error thrown when a requested resource (e.g. job) is not found (HTTP 404).
 */
export declare class OrchestrixNotFoundError extends OrchestrixApiError {
    constructor(statusText: string, body: any, message?: string);
}
/**
 * Thrown when an HTTP request exceeds the specified timeout duration.
 */
export declare class OrchestrixTimeoutError extends OrchestrixError {
    readonly timeoutMs: number;
    constructor(message: string, timeoutMs: number);
}
/**
 * Thrown when a network connection failure occurs (e.g. server unreachable, connection refused, DNS failure).
 */
export declare class OrchestrixConnectionError extends OrchestrixError {
    readonly cause?: Error;
    constructor(message: string, cause?: Error);
}
//# sourceMappingURL=errors.d.ts.map