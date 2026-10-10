"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.OrchestrixClient = exports.JobsResource = void 0;
const errors_1 = require("./errors");
const DEFAULT_TIMEOUT_MS = 10000;
class JobsResource {
    client;
    constructor(client) {
        this.client = client;
    }
    /**
     * Enqueues a new job on the Orchestrix server.
     *
     * @param data Job payload object containing job parameter data.
     * @param options Optional priority, delay, idempotencyKey, and per-request timeoutMs.
     * @returns Promise resolving to the created job response.
     */
    async create(data, options = {}) {
        if (!data || typeof data !== 'object' || Array.isArray(data)) {
            throw new errors_1.OrchestrixValidationError('Job data must be a valid non-null object');
        }
        if (options.priority !== undefined) {
            if (!Number.isInteger(options.priority) || options.priority < 1 || options.priority > 10) {
                throw new errors_1.OrchestrixValidationError(`Invalid priority "${options.priority}". Priority must be an integer between 1 and 10.`);
            }
        }
        if (options.delay !== undefined) {
            if (typeof options.delay !== 'number' || options.delay < 0 || isNaN(options.delay)) {
                throw new errors_1.OrchestrixValidationError(`Invalid delay "${options.delay}". Delay must be a non-negative number in milliseconds.`);
            }
        }
        const payload = { ...data };
        if (options.priority !== undefined) {
            payload.priority = options.priority;
        }
        if (options.delay !== undefined) {
            payload.delay = options.delay;
        }
        const headers = {
            'Content-Type': 'application/json',
        };
        if (options.idempotencyKey && options.idempotencyKey.trim().length > 0) {
            headers['Idempotency-Key'] = options.idempotencyKey.trim();
        }
        return this.client.request('/jobs', {
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
    async get(jobId, options = {}) {
        if (!jobId || typeof jobId !== 'string' || jobId.trim().length === 0) {
            throw new errors_1.OrchestrixValidationError('jobId must be a non-empty string');
        }
        const sanitizedId = jobId.trim();
        return this.client.request(`/jobs/${encodeURIComponent(sanitizedId)}`, {
            method: 'GET',
            timeoutMs: options.timeoutMs,
        });
    }
}
exports.JobsResource = JobsResource;
class OrchestrixClient {
    baseUrl;
    timeoutMs;
    jobs;
    constructor(options) {
        let rawBaseUrl;
        let timeout = DEFAULT_TIMEOUT_MS;
        if (typeof options === 'string') {
            rawBaseUrl = options;
        }
        else if (options && typeof options === 'object') {
            rawBaseUrl = options.baseUrl;
            if (options.timeoutMs !== undefined) {
                if (typeof options.timeoutMs !== 'number' || options.timeoutMs <= 0 || isNaN(options.timeoutMs)) {
                    throw new errors_1.OrchestrixValidationError(`Invalid timeoutMs "${options.timeoutMs}". Must be a positive number.`);
                }
                timeout = options.timeoutMs;
            }
        }
        else {
            throw new errors_1.OrchestrixValidationError('OrchestrixClient requires a baseUrl string or configuration object containing baseUrl');
        }
        if (!rawBaseUrl || typeof rawBaseUrl !== 'string' || rawBaseUrl.trim().length === 0) {
            throw new errors_1.OrchestrixValidationError('baseUrl must be a non-empty string');
        }
        try {
            const parsedUrl = new URL(rawBaseUrl.trim());
            if (parsedUrl.protocol !== 'http:' && parsedUrl.protocol !== 'https:') {
                throw new errors_1.OrchestrixValidationError(`Invalid protocol "${parsedUrl.protocol}" in baseUrl. Must be http or https.`);
            }
        }
        catch (err) {
            if (err instanceof errors_1.OrchestrixValidationError) {
                throw err;
            }
            throw new errors_1.OrchestrixValidationError(`Invalid baseUrl URL string "${rawBaseUrl}": ${err.message}`);
        }
        // Strip trailing slashes for predictable path concatenation
        this.baseUrl = rawBaseUrl.trim().replace(/\/+$/, '');
        this.timeoutMs = timeout;
        this.jobs = new JobsResource(this);
    }
    /**
     * Internal helper method to execute HTTP requests with timeout, error handling, and type parsing.
     */
    async request(path, init) {
        const url = `${this.baseUrl}${path}`;
        const effectiveTimeout = init.timeoutMs ?? this.timeoutMs;
        const controller = new AbortController();
        let isTimeoutTriggered = false;
        const timeoutId = setTimeout(() => {
            isTimeoutTriggered = true;
            controller.abort();
        }, effectiveTimeout);
        let res;
        try {
            res = await fetch(url, {
                method: init.method,
                headers: init.headers,
                body: init.body,
                signal: controller.signal,
            });
        }
        catch (err) {
            if (isTimeoutTriggered || controller.signal.aborted) {
                throw new errors_1.OrchestrixTimeoutError(`Request to ${url} timed out after ${effectiveTimeout}ms`, effectiveTimeout);
            }
            throw new errors_1.OrchestrixConnectionError(`Failed to connect to Orchestrix server at ${url}: ${err?.message || err}`, err instanceof Error ? err : undefined);
        }
        finally {
            clearTimeout(timeoutId);
        }
        let responseData;
        const contentType = res.headers.get('content-type') || '';
        if (contentType.includes('application/json')) {
            try {
                responseData = await res.json();
            }
            catch (jsonErr) {
                responseData = null;
            }
        }
        else {
            try {
                responseData = await res.text();
            }
            catch (textErr) {
                responseData = null;
            }
        }
        if (!res.ok) {
            if (res.status === 404) {
                throw new errors_1.OrchestrixNotFoundError(res.statusText, responseData);
            }
            throw new errors_1.OrchestrixApiError(res.status, res.statusText, responseData);
        }
        return responseData;
    }
}
exports.OrchestrixClient = OrchestrixClient;
//# sourceMappingURL=client.js.map