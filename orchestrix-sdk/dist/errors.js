"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.OrchestrixConnectionError = exports.OrchestrixTimeoutError = exports.OrchestrixNotFoundError = exports.OrchestrixApiError = exports.OrchestrixValidationError = exports.OrchestrixError = void 0;
/**
 * Base class for all Orchestrix SDK errors.
 */
class OrchestrixError extends Error {
    constructor(message) {
        super(message);
        this.name = 'OrchestrixError';
        Object.setPrototypeOf(this, new.target.prototype);
    }
}
exports.OrchestrixError = OrchestrixError;
/**
 * Thrown when client-side input validation fails (e.g. invalid baseUrl, empty jobId).
 */
class OrchestrixValidationError extends OrchestrixError {
    constructor(message) {
        super(message);
        this.name = 'OrchestrixValidationError';
    }
}
exports.OrchestrixValidationError = OrchestrixValidationError;
/**
 * Thrown when a request to the Orchestrix API returns an HTTP error status (4xx or 5xx).
 */
class OrchestrixApiError extends OrchestrixError {
    status;
    statusText;
    body;
    constructor(status, statusText, body, message) {
        const errorMsg = message ||
            (typeof body === 'object' && body?.error ? body.error : `HTTP ${status} ${statusText}`);
        super(errorMsg);
        this.name = 'OrchestrixApiError';
        this.status = status;
        this.statusText = statusText;
        this.body = body;
    }
}
exports.OrchestrixApiError = OrchestrixApiError;
/**
 * Specific error thrown when a requested resource (e.g. job) is not found (HTTP 404).
 */
class OrchestrixNotFoundError extends OrchestrixApiError {
    constructor(statusText, body, message) {
        super(404, statusText, body, message);
        this.name = 'OrchestrixNotFoundError';
    }
}
exports.OrchestrixNotFoundError = OrchestrixNotFoundError;
/**
 * Thrown when an HTTP request exceeds the specified timeout duration.
 */
class OrchestrixTimeoutError extends OrchestrixError {
    timeoutMs;
    constructor(message, timeoutMs) {
        super(message);
        this.name = 'OrchestrixTimeoutError';
        this.timeoutMs = timeoutMs;
    }
}
exports.OrchestrixTimeoutError = OrchestrixTimeoutError;
/**
 * Thrown when a network connection failure occurs (e.g. server unreachable, connection refused, DNS failure).
 */
class OrchestrixConnectionError extends OrchestrixError {
    cause;
    constructor(message, cause) {
        super(message);
        this.name = 'OrchestrixConnectionError';
        this.cause = cause;
    }
}
exports.OrchestrixConnectionError = OrchestrixConnectionError;
//# sourceMappingURL=errors.js.map