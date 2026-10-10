import { OrchestrixClientOptions, CreateJobOptions, CreateJobResponse, JobDetails, GetJobOptions } from './types';
export declare class JobsResource {
    private readonly client;
    constructor(client: OrchestrixClient);
    /**
     * Enqueues a new job on the Orchestrix server.
     *
     * @param data Job payload object containing job parameter data.
     * @param options Optional priority, delay, idempotencyKey, and per-request timeoutMs.
     * @returns Promise resolving to the created job response.
     */
    create(data: Record<string, any>, options?: CreateJobOptions): Promise<CreateJobResponse>;
    /**
     * Retrieves status and details of a job by ID.
     *
     * @param jobId Unique ID of the job to fetch.
     * @param options Optional per-request options (e.g. timeoutMs).
     * @returns Promise resolving to JobDetails object.
     */
    get(jobId: string, options?: GetJobOptions): Promise<JobDetails>;
}
export declare class OrchestrixClient {
    readonly baseUrl: string;
    readonly timeoutMs: number;
    readonly jobs: JobsResource;
    constructor(options: string | OrchestrixClientOptions);
    /**
     * Internal helper method to execute HTTP requests with timeout, error handling, and type parsing.
     */
    request<T>(path: string, init: {
        method: 'GET' | 'POST';
        headers?: Record<string, string>;
        body?: string;
        timeoutMs?: number;
    }): Promise<T>;
}
//# sourceMappingURL=client.d.ts.map