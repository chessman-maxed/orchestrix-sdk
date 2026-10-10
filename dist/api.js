"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.reconcileJobStatus = reconcileJobStatus;
const express_1 = __importDefault(require("express"));
const crypto_1 = __importDefault(require("crypto"));
const queue_1 = require("./queue");
const redis_1 = require("./redis");
const db_1 = require("./db");
const scheduler_1 = require("./scheduler");
const app = (0, express_1.default)();
app.use(express_1.default.json());
const PORT = process.env.PORT || 3000;
const IDEMPOTENCY_TTL_SECONDS = 86400; // 24 hours
/**
 * Helper function to parse and validate job priority values (1-10 integer, defaults to 5).
 */
function parsePriority(val) {
    if (val === undefined || val === null) {
        return 5;
    }
    const num = Number(val);
    if (!Number.isInteger(num) || num < 1 || num > 10) {
        return 5;
    }
    return num;
}
/**
 * Helper function to parse and validate job delay values in milliseconds (positive number).
 * Returns undefined if omitted, non-numeric, or <= 0 (causing immediate execution).
 */
function parseDelay(val) {
    if (val === undefined || val === null) {
        return undefined;
    }
    const num = Number(val);
    if (isNaN(num) || num <= 0) {
        return undefined;
    }
    return num;
}
/**
 * 1. POST /jobs
 * Accepts a JSON body and pushes a new job to the Redis queue.
 * Supports optional HTTP header 'Idempotency-Key' for atomic request deduplication.
 */
app.post('/jobs', async (req, res) => {
    try {
        const payload = req.body;
        const priority = parsePriority(payload?.priority);
        const delay = parseDelay(payload?.delay);
        // Read Idempotency-Key ONLY from HTTP headers
        const rawHeaderKey = req.headers['idempotency-key'];
        const idempotencyKey = typeof rawHeaderKey === 'string' && rawHeaderKey.trim().length > 0
            ? rawHeaderKey.trim()
            : undefined;
        if (idempotencyKey) {
            const redisKey = `idempotency:${idempotencyKey}`;
            // Atomic reservation using SET NX with 24-hour TTL (86,400s) to prevent race conditions
            const lockAcquired = await redis_1.redisClient.set(redisKey, 'PROCESSING', 'EX', IDEMPOTENCY_TTL_SECONDS, 'NX');
            if (!lockAcquired) {
                // Key already exists: another request locked or completed this idempotency key
                let existingJobId = await redis_1.redisClient.get(redisKey);
                let retries = 0;
                // Wait/poll if initial job creation is still in progress ('PROCESSING')
                while (existingJobId === 'PROCESSING' && retries < 20) {
                    await new Promise((resolve) => setTimeout(resolve, 250));
                    existingJobId = await redis_1.redisClient.get(redisKey);
                    retries++;
                }
                if (existingJobId && existingJobId !== 'PROCESSING') {
                    const existingJob = await queue_1.taskQueue.getJob(existingJobId);
                    if (existingJob) {
                        const state = await existingJob.getState();
                        res.status(200).json({
                            message: 'Duplicate request detected. Returning existing job.',
                            jobId: existingJob.id,
                            isDuplicate: true,
                            name: existingJob.name,
                            state: state,
                            priority: existingJob.opts.priority || 5,
                            delay: existingJob.opts.delay || 0,
                            attemptsMade: existingJob.attemptsMade,
                            maxAttempts: existingJob.opts.attempts,
                            data: existingJob.data,
                            progress: existingJob.progress,
                            failedReason: existingJob.failedReason,
                            returnvalue: existingJob.returnvalue,
                            timestamp: existingJob.timestamp,
                            finishedOn: existingJob.finishedOn,
                        });
                        return;
                    }
                }
            }
            else {
                // Lock acquired successfully: we are responsible for creating the job
                try {
                    const job = await queue_1.taskQueue.add('default-job', payload, { priority, delay });
                    // Store created jobId in Redis under the idempotency key with 24-hour TTL
                    await redis_1.redisClient.set(redisKey, job.id, 'EX', IDEMPOTENCY_TTL_SECONDS);
                    // Best-effort PostgreSQL insert
                    try {
                        await (0, db_1.insertJob)({
                            bullmqJobId: job.id,
                            taskType: job.name,
                            priority: job.opts.priority ?? priority,
                            maxAttempts: job.opts.attempts ?? 3,
                        });
                    }
                    catch (dbError) {
                        console.error('[DB] ❌ Failed to insert job record into PostgreSQL (idempotency path):', dbError);
                    }
                    res.status(201).json({
                        message: 'Job submitted successfully',
                        jobId: job.id,
                        name: job.name,
                        priority: job.opts.priority || priority,
                        delay: job.opts.delay || 0,
                        isDuplicate: false,
                        data: job.data,
                    });
                    return;
                }
                catch (addError) {
                    // Cleanup reserved key on job creation failure so no broken reservation remains
                    await redis_1.redisClient.del(redisKey);
                    throw addError;
                }
            }
        }
        // Standard path for requests without Idempotency-Key header
        const job = await queue_1.taskQueue.add('default-job', payload, { priority, delay });
        // Best-effort PostgreSQL insert
        try {
            await (0, db_1.insertJob)({
                bullmqJobId: job.id,
                taskType: job.name,
                priority: job.opts.priority ?? priority,
                maxAttempts: job.opts.attempts ?? 3,
            });
        }
        catch (dbError) {
            console.error('[DB] ❌ Failed to insert job record into PostgreSQL (standard path):', dbError);
        }
        res.status(201).json({
            message: 'Job submitted successfully',
            jobId: job.id,
            name: job.name,
            priority: job.opts.priority || priority,
            delay: job.opts.delay || 0,
            isDuplicate: false,
            data: job.data,
        });
    }
    catch (error) {
        console.error('Error submitting job:', error);
        res.status(500).json({ error: 'Failed to submit job to queue' });
    }
});
/**
 * 2. POST /jobs/bulk
 * Submits jobs in bulk as a batch. Creates a PostgreSQL batch record and enqueues individual jobs.
 * Supports optional HTTP header 'Idempotency-Key' for request deduplication.
 */
app.post('/jobs/bulk', async (req, res) => {
    try {
        let name = req.body.name;
        let jobsInput = req.body.jobs;
        // Backward compatibility for demo payload { count: N, priority: P }
        if (!jobsInput && req.body.count !== undefined) {
            const count = parseInt(req.body.count, 10);
            const priority = parsePriority(req.body.priority);
            if (isNaN(count) || count <= 0 || count > 100) {
                res.status(400).json({ error: 'Please provide a valid count integer between 1 and 100' });
                return;
            }
            name = name || `Bulk Batch (${count} jobs)`;
            jobsInput = [];
            for (let i = 1; i <= count; i++) {
                jobsInput.push({
                    task: 'bulk-job',
                    payload: { bulkIndex: i, createdAt: new Date().toISOString() },
                    priority,
                });
            }
        }
        // Validation
        if (!name || typeof name !== 'string' || name.trim().length === 0) {
            res.status(400).json({ error: 'Please provide a valid non-empty string for batch "name"' });
            return;
        }
        if (!Array.isArray(jobsInput) || jobsInput.length === 0) {
            res.status(400).json({ error: 'Please provide a non-empty "jobs" array' });
            return;
        }
        if (jobsInput.length > 100) {
            res.status(400).json({ error: 'Batch size exceeds maximum limit of 100 jobs' });
            return;
        }
        for (let i = 0; i < jobsInput.length; i++) {
            const jobItem = jobsInput[i];
            const taskName = jobItem.task || jobItem.taskType;
            if (!taskName || typeof taskName !== 'string' || taskName.trim().length === 0) {
                res.status(400).json({ error: `Job at index ${i} is missing a valid "task" string parameter` });
                return;
            }
        }
        // Read Idempotency-Key ONLY from HTTP headers
        const rawHeaderKey = req.headers['idempotency-key'];
        const idempotencyKey = typeof rawHeaderKey === 'string' && rawHeaderKey.trim().length > 0
            ? rawHeaderKey.trim()
            : undefined;
        if (idempotencyKey) {
            const redisKey = `idempotency:bulk:${idempotencyKey}`;
            const lockAcquired = await redis_1.redisClient.set(redisKey, 'PROCESSING', 'EX', IDEMPOTENCY_TTL_SECONDS, 'NX');
            if (!lockAcquired) {
                let existingBatchId = await redis_1.redisClient.get(redisKey);
                let retries = 0;
                while (existingBatchId === 'PROCESSING' && retries < 20) {
                    await new Promise((resolve) => setTimeout(resolve, 250));
                    existingBatchId = await redis_1.redisClient.get(redisKey);
                    retries++;
                }
                if (existingBatchId && existingBatchId !== 'PROCESSING') {
                    const existingBatch = await (0, db_1.getJobBatchById)(existingBatchId);
                    if (existingBatch) {
                        const batchJobs = await (0, db_1.getBatchJobs)(existingBatch.id);
                        res.status(200).json({
                            message: 'Duplicate request detected. Returning existing batch.',
                            isDuplicate: true,
                            batchId: String(existingBatch.id),
                            name: existingBatch.name,
                            status: existingBatch.status,
                            totalJobs: existingBatch.total_jobs,
                            jobIds: batchJobs.map((j) => j.bullmq_job_id),
                        });
                        return;
                    }
                }
            }
            else {
                try {
                    const batch = await (0, db_1.createJobBatch)(name.trim(), jobsInput.length);
                    await redis_1.redisClient.set(redisKey, String(batch.id), 'EX', IDEMPOTENCY_TTL_SECONDS);
                    const createdJobIds = [];
                    for (const item of jobsInput) {
                        const taskType = item.task || item.taskType || 'default-job';
                        const priority = parsePriority(item.priority);
                        const delay = parseDelay(item.delay);
                        const payload = item.payload ?? {};
                        try {
                            const job = await queue_1.taskQueue.add(taskType, payload, { priority, delay });
                            createdJobIds.push(job.id);
                            try {
                                await (0, db_1.insertJob)({
                                    bullmqJobId: job.id,
                                    taskType: job.name,
                                    priority: job.opts.priority ?? priority,
                                    maxAttempts: job.opts.attempts ?? 3,
                                    batchId: batch.id,
                                });
                            }
                            catch (dbError) {
                                console.error(`[DB] ❌ Failed to insert batch job ${job.id} into PostgreSQL:`, dbError);
                            }
                        }
                        catch (enqueueErr) {
                            console.error(`[Bulk API] ❌ Failed to enqueue job for batch ${batch.id}:`, enqueueErr);
                        }
                    }
                    const updatedBatch = await (0, db_1.updateBatchProgress)(batch.id);
                    res.status(201).json({
                        batchId: String(batch.id),
                        name: batch.name,
                        status: updatedBatch?.status || 'pending',
                        totalJobs: updatedBatch?.total_jobs ?? createdJobIds.length,
                        jobIds: createdJobIds,
                    });
                    return;
                }
                catch (err) {
                    await redis_1.redisClient.del(redisKey);
                    throw err;
                }
            }
        }
        // Standard path without Idempotency-Key
        const batch = await (0, db_1.createJobBatch)(name.trim(), jobsInput.length);
        const createdJobIds = [];
        for (const item of jobsInput) {
            const taskType = item.task || item.taskType || 'default-job';
            const priority = parsePriority(item.priority);
            const delay = parseDelay(item.delay);
            const payload = item.payload ?? {};
            try {
                const job = await queue_1.taskQueue.add(taskType, payload, { priority, delay });
                createdJobIds.push(job.id);
                try {
                    await (0, db_1.insertJob)({
                        bullmqJobId: job.id,
                        taskType: job.name,
                        priority: job.opts.priority ?? priority,
                        maxAttempts: job.opts.attempts ?? 3,
                        batchId: batch.id,
                    });
                }
                catch (dbError) {
                    console.error(`[DB] ❌ Failed to insert batch job ${job.id} into PostgreSQL:`, dbError);
                }
            }
            catch (enqueueErr) {
                console.error(`[Bulk API] ❌ Failed to enqueue job for batch ${batch.id}:`, enqueueErr);
            }
        }
        const updatedBatch = await (0, db_1.updateBatchProgress)(batch.id);
        res.status(201).json({
            batchId: String(batch.id),
            name: batch.name,
            status: updatedBatch?.status || 'pending',
            totalJobs: updatedBatch?.total_jobs ?? createdJobIds.length,
            jobIds: createdJobIds,
        });
    }
    catch (error) {
        console.error('Error submitting bulk jobs:', error);
        res.status(500).json({ error: 'Failed to submit bulk jobs to queue' });
    }
});
/**
 * Reconciliation helper for jobs whose execution was stopped during cancellation
 * but whose PostgreSQL status update was interrupted due to DB connectivity.
 */
async function reconcileJobStatus(bullmqJobId) {
    try {
        const lockKey = `job:reconcile:lock:${bullmqJobId}`;
        const lockAcquired = await redis_1.redisClient.set(lockKey, '1', 'EX', 15, 'NX');
        if (!lockAcquired) {
            return await (0, db_1.getJobByBullId)(bullmqJobId);
        }
        try {
            const markerRaw = await redis_1.redisClient.get(`job:cancel:durable:${bullmqJobId}`);
            const dbJob = await (0, db_1.getJobByBullId)(bullmqJobId);
            if (markerRaw && dbJob) {
                const marker = JSON.parse(markerRaw);
                if (dbJob.status === 'running') {
                    const { rowCount, batchId } = await (0, db_1.cancelJobInDb)({
                        bullmqJobId,
                        cancelledBy: marker.cancelledBy || 'system_reconciliation',
                        reason: marker.reason || 'Durable cancellation reconciliation',
                    });
                    if (rowCount === 1) {
                        if (batchId) {
                            await (0, db_1.updateBatchProgress)(batchId);
                        }
                        await redis_1.redisClient.del(`job:cancel:durable:${bullmqJobId}`);
                        return await (0, db_1.getJobByBullId)(bullmqJobId);
                    }
                }
                else if (dbJob.status === 'completed' ||
                    dbJob.status === 'failed' ||
                    dbJob.status === 'dead_lettered' ||
                    dbJob.status === 'cancelled') {
                    await redis_1.redisClient.del(`job:cancel:durable:${bullmqJobId}`);
                }
            }
            return dbJob;
        }
        finally {
            await redis_1.redisClient.del(lockKey);
        }
    }
    catch (err) {
        console.error(`[API] Error during job status reconciliation for ${bullmqJobId}:`, err);
        return await (0, db_1.getJobByBullId)(bullmqJobId);
    }
}
/**
 * 3. GET /jobs/:id
 * Fetches the status and details of a job by its ID.
 */
app.get('/jobs/:id', async (req, res) => {
    try {
        const { id } = req.params;
        // Trigger on-demand reconciliation if a durable cancellation marker exists
        const dbJob = await reconcileJobStatus(id);
        // Retrieve the job instance from Redis using BullMQ
        const job = await queue_1.taskQueue.getJob(id);
        if (!job && !dbJob) {
            res.status(404).json({ error: `Job with ID ${id} not found` });
            return;
        }
        // Get current state (waiting, active, completed, failed, delayed)
        const state = dbJob?.status === 'cancelled'
            ? 'cancelled'
            : (job ? await job.getState() : (dbJob?.status || 'unknown'));
        res.status(200).json({
            id: id,
            state: state,
            dbStatus: dbJob?.status || null,
            priority: job?.opts.priority || dbJob?.priority || 5,
            delay: job?.opts.delay || 0,
            attemptsMade: job?.attemptsMade || dbJob?.attempts || 0,
            maxAttempts: job?.opts.attempts || dbJob?.max_attempts || 3,
            data: job?.data || null,
            progress: job?.progress || 0,
            failedReason: job?.failedReason || dbJob?.failed_reason || null,
            cancelledAt: dbJob?.cancelled_at || null,
            cancelledBy: dbJob?.cancelled_by || null,
            cancellationReason: dbJob?.cancellation_reason || null,
            returnvalue: job?.returnvalue || null,
            timestamp: job?.timestamp || (dbJob?.created_at ? new Date(dbJob.created_at).getTime() : null),
            finishedOn: job?.finishedOn || (dbJob?.completed_at ? new Date(dbJob.completed_at).getTime() : null),
        });
    }
    catch (error) {
        console.error('Error fetching job status:', error);
        res.status(500).json({ error: 'Failed to fetch job status' });
    }
});
/**
 * POST /jobs/:id/cancel
 * Phase 2 Active Job Cancellation: Cancels waiting, delayed, retrying, OR actively running jobs.
 * For active running jobs, coordinates via Redis Pub/Sub, AbortController signal, and two-phase ACK protocol.
 */
app.post('/jobs/:id/cancel', async (req, res) => {
    try {
        const { id } = req.params;
        const { reason, cancelledBy } = req.body || {};
        let dbJob = await (0, db_1.getJobByBullId)(id);
        const bullmqJob = await queue_1.taskQueue.getJob(id);
        if (!dbJob && !bullmqJob) {
            res.status(404).json({ error: `Job with ID ${id} not found` });
            return;
        }
        let currentDbStatus = dbJob?.status;
        // Reject already terminal jobs
        if (currentDbStatus === 'completed' ||
            currentDbStatus === 'failed' ||
            currentDbStatus === 'dead_lettered' ||
            currentDbStatus === 'cancelled') {
            res.status(400).json({
                error: 'Bad Request',
                message: `Cannot cancel job ${id} because it is already in terminal state '${currentDbStatus}'`,
                status: currentDbStatus,
            });
            return;
        }
        // Check BullMQ state and DB status for active running jobs
        let bullmqState;
        if (bullmqJob) {
            bullmqState = await bullmqJob.getState();
        }
        // Phase 2 Active Cancellation Protocol for running / active jobs
        if (currentDbStatus === 'running' || bullmqState === 'active') {
            const requestId = `req_cancel_${id}_${Date.now()}_${crypto_1.default.randomUUID()}`;
            const lockKey = `job:cancel:in_flight:${id}`;
            // Single-flight cancellation request lock (NX EX 30)
            const lockAcquired = await redis_1.redisClient.set(lockKey, requestId, 'EX', 30, 'NX');
            if (!lockAcquired) {
                res.status(409).json({
                    error: 'Conflict',
                    message: `Cancellation already in progress for job ${id}`,
                    status: 'running',
                });
                return;
            }
            const subscriber = (0, redis_1.createRedisClient)();
            let timer = null;
            try {
                await subscriber.subscribe(`channel:job:cancel:resp:${requestId}`);
                const ackPromise = new Promise((resolve, reject) => {
                    timer = setTimeout(() => {
                        reject(new Error('TIMEOUT'));
                    }, 5000);
                    subscriber.on('message', (channel, messageStr) => {
                        if (channel !== `channel:job:cancel:resp:${requestId}`)
                            return;
                        try {
                            const msg = JSON.parse(messageStr);
                            if (msg.status === 'STOPPED' || msg.status === 'RACE_LOST') {
                                if (timer)
                                    clearTimeout(timer);
                                resolve(msg);
                            }
                        }
                        catch (e) {
                            // ignore parse errors
                        }
                    });
                });
                // Publish cancellation request to worker pool
                await redis_1.redisClient.publish('channel:job:cancel:req', JSON.stringify({
                    requestId,
                    jobId: id,
                    cancelledBy: typeof cancelledBy === 'string' ? cancelledBy : 'user',
                    reason: typeof reason === 'string' ? reason : undefined,
                    timestamp: new Date().toISOString(),
                }));
                const result = await ackPromise;
                if (result.status === 'STOPPED') {
                    res.status(200).json({
                        message: 'Active job cancelled successfully',
                        jobId: id,
                        previousStatus: currentDbStatus || 'running',
                        status: 'cancelled',
                        cancelledAt: new Date().toISOString(),
                    });
                    return;
                }
                else if (result.status === 'RACE_LOST') {
                    res.status(409).json({
                        error: 'Conflict',
                        message: `Job ${id} completed naturally before cancellation could stop it`,
                        status: result.finalStatus || 'completed',
                    });
                    return;
                }
            }
            catch (err) {
                if (err?.message === 'TIMEOUT') {
                    res.status(504).json({
                        error: 'Gateway Timeout',
                        message: `Worker acknowledged cancellation request, but execution stoppage could not be confirmed within 5000ms`,
                        jobId: id,
                        status: 'running',
                    });
                    return;
                }
                console.error(`[API] Redis publish/subscribe error for cancel job ${id}:`, err);
                res.status(503).json({ error: 'Service Unavailable', message: 'Failed to communicate cancellation request via Redis' });
                return;
            }
            finally {
                if (timer)
                    clearTimeout(timer);
                await (0, redis_1.releaseCancelLock)(lockKey, requestId);
                try {
                    await subscriber.unsubscribe();
                    await subscriber.quit();
                }
                catch (cleanErr) {
                    console.error('[API] Error cleaning up subscriber:', cleanErr);
                }
            }
        }
        // Phase 1 Cancellation for non-active jobs (waiting, delayed, prioritized)
        if (bullmqJob && (bullmqState === 'waiting' || bullmqState === 'delayed' || bullmqState === 'prioritized')) {
            try {
                await bullmqJob.remove();
            }
            catch (removeErr) {
                console.warn(`[API] Warning: Failed to remove job ${id} from BullMQ queue directly:`, removeErr);
            }
        }
        // Update PostgreSQL job state to 'cancelled'
        const { batchId } = await (0, db_1.cancelJobInDb)({
            bullmqJobId: id,
            cancelledBy: typeof cancelledBy === 'string' ? cancelledBy : 'user',
            reason: typeof reason === 'string' ? reason : undefined,
        });
        if (batchId) {
            await (0, db_1.updateBatchProgress)(batchId);
        }
        res.status(200).json({
            message: 'Job cancelled successfully',
            jobId: id,
            previousStatus: currentDbStatus || 'pending',
            status: 'cancelled',
            cancelledAt: new Date().toISOString(),
        });
    }
    catch (error) {
        console.error(`Error cancelling job ${req.params.id}:`, error);
        res.status(500).json({ error: 'Failed to cancel job' });
    }
});
/**
 * 4. GET /jobs-status/failed
 * Fetches all currently failed jobs from the queue.
 */
app.get('/jobs-status/failed', async (req, res) => {
    try {
        const failedJobs = await queue_1.taskQueue.getFailed();
        const result = failedJobs.map((job) => ({
            id: job.id,
            name: job.name,
            priority: job.opts.priority || 5,
            data: job.data,
            failedReason: job.failedReason,
            attemptsMade: job.attemptsMade,
            timestamp: job.timestamp,
        }));
        res.status(200).json({
            count: failedJobs.length,
            failedJobs: result,
        });
    }
    catch (error) {
        console.error('Error fetching failed jobs:', error);
        res.status(500).json({ error: 'Failed to fetch failed jobs list' });
    }
});
/**
 * 5. POST /jobs/:id/retry
 * Manually replays a failed or dead-lettered job by creating a new job cycle in orchestrix-queue
 * while preserving the original job payload, original job history, and DLQ snapshot intact.
 * Stored lineage replayedFromJobId uses the canonical original BullMQ job ID.
 * Replayed jobs execute immediately (delay is reset to 0 / omitted).
 */
app.post('/jobs/:id/retry', async (req, res) => {
    try {
        const { id } = req.params;
        // Standardize input ID: canonical ID is '53', but support 'dlq:53' or 'dlq-53' passed accidentally
        const canonicalJobId = id.startsWith('dlq:')
            ? id.replace('dlq:', '')
            : id.startsWith('dlq-')
                ? id.replace('dlq-', '')
                : id;
        // Search for original job in primary taskQueue or DLQ snapshot
        const originalJob = await queue_1.taskQueue.getJob(canonicalJobId);
        const dlqJob = await queue_1.dlqQueue.getJob(`dlq-${canonicalJobId}`);
        if (!originalJob && !dlqJob) {
            res.status(404).json({ error: `Job with ID ${canonicalJobId} not found in task queue or DLQ` });
            return;
        }
        let jobData;
        let taskName = 'default-job';
        let originalPriority = 5;
        let failedReason = null;
        if (originalJob) {
            const state = await originalJob.getState();
            if (state !== 'failed' && !dlqJob) {
                res.status(400).json({ error: `Cannot retry job ${canonicalJobId} because its current state is '${state}', not 'failed' or 'dead_lettered'` });
                return;
            }
            jobData = originalJob.data;
            taskName = originalJob.name || 'default-job';
            originalPriority = originalJob.opts.priority || 5;
            failedReason = originalJob.failedReason || null;
        }
        else if (dlqJob) {
            jobData = dlqJob.data.payload || dlqJob.data;
            taskName = dlqJob.data.taskType || 'default-job';
            originalPriority = dlqJob.data.priority || 5;
            failedReason = dlqJob.data.failedReason || null;
        }
        // Preserve exact original payload and augment with parent link metadata
        const replayedPayload = {
            ...jobData,
            _replayedFromJobId: canonicalJobId,
            _previousFailedReason: failedReason,
        };
        // Add a new job to start a fresh retry cycle in orchestrix-queue inheriting original priority
        const newJob = await queue_1.taskQueue.add(taskName, replayedPayload, {
            priority: originalPriority,
        });
        // Best-effort PostgreSQL insert for replay job (referencing canonical originalJobId and preserving batchId)
        const dbJob = await (0, db_1.getJobByBullId)(canonicalJobId);
        const batchId = dbJob?.batch_id ? Number(dbJob.batch_id) : undefined;
        try {
            await (0, db_1.insertJob)({
                bullmqJobId: newJob.id,
                taskType: newJob.name,
                priority: newJob.opts.priority ?? originalPriority,
                maxAttempts: newJob.opts.attempts ?? 3,
                replayedFromJobId: canonicalJobId,
                batchId,
            });
            if (batchId) {
                await (0, db_1.updateBatchProgress)(batchId);
            }
        }
        catch (dbError) {
            console.error(`[DB] ❌ Failed to insert replay job ${newJob.id} into PostgreSQL:`, dbError);
        }
        res.status(201).json({
            message: `Manual replay initiated for job ${canonicalJobId}. Created new job cycle ${newJob.id}.`,
            jobId: newJob.id,
            replayedFromJobId: canonicalJobId,
            priority: newJob.opts.priority || originalPriority,
        });
    }
    catch (error) {
        console.error('Error retrying job:', error);
        res.status(500).json({ error: 'Failed to manually retry job' });
    }
});
/**
 * 6. GET /dlq
 * Lists all jobs currently sitting in the Dead Letter Queue.
 */
app.get('/dlq', async (req, res) => {
    try {
        const jobs = await queue_1.dlqQueue.getJobs(['waiting', 'active', 'completed', 'failed', 'delayed']);
        const result = jobs.map((job) => ({
            id: job.id,
            name: job.name,
            data: job.data,
            timestamp: job.timestamp,
        }));
        res.status(200).json({
            count: result.length,
            dlqJobs: result,
        });
    }
    catch (error) {
        console.error('Error fetching DLQ jobs:', error);
        res.status(500).json({ error: 'Failed to fetch DLQ jobs list' });
    }
});
/**
 * Helper function to retrieve a job from dlqQueue by trying dlq-${id}, exact ${id}, or matching originalJobId.
 */
async function findDlqJob(idParam) {
    const normalizedId = idParam.trim();
    const dlqJobId = normalizedId.startsWith('dlq-')
        ? normalizedId
        : normalizedId.startsWith('dlq:')
            ? normalizedId.replace('dlq:', 'dlq-')
            : `dlq-${normalizedId}`;
    // 1. Try direct lookup with dlq-${id}
    let job = await queue_1.dlqQueue.getJob(dlqJobId);
    if (job)
        return job;
    // 2. Try lookup with exact param ID
    job = await queue_1.dlqQueue.getJob(normalizedId);
    if (job)
        return job;
    // 3. Fallback: Search all active DLQ jobs by matching job.id or originalJobId
    const allJobs = await queue_1.dlqQueue.getJobs(['waiting', 'active', 'completed', 'failed', 'delayed']);
    return allJobs.find((j) => j.id === dlqJobId ||
        j.id === normalizedId ||
        String(j.data?.originalJobId) === normalizedId);
}
/**
 * 7. GET /dlq/:id
 * Fetches details of a specific job snapshot from the Dead Letter Queue.
 */
app.get('/dlq/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const job = await findDlqJob(id);
        if (!job) {
            res.status(404).json({ error: `DLQ job with ID ${id} not found` });
            return;
        }
        res.status(200).json({
            id: job.id,
            name: job.name,
            data: job.data,
            timestamp: job.timestamp,
        });
    }
    catch (error) {
        console.error('Error fetching DLQ job details:', error);
        res.status(500).json({ error: 'Failed to fetch DLQ job details' });
    }
});
app.delete('/dlq/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const job = await findDlqJob(id);
        if (!job) {
            res.status(404).json({ error: `DLQ job with ID ${id} not found` });
            return;
        }
        await job.remove();
        res.status(200).json({
            message: `DLQ entry ${id} removed successfully from Dead Letter Queue. Original database history preserved.`,
            dlqJobId: job.id,
        });
    }
    catch (error) {
        console.error('Error removing DLQ job:', error);
        res.status(500).json({ error: 'Failed to remove job from DLQ' });
    }
});
/**
 * 9. POST /schedules
 * Creates a new persistent recurring schedule.
 */
app.post('/schedules', async (req, res) => {
    try {
        const { name, task, intervalMs, priority, payload } = req.body;
        if (!task || typeof task !== 'string') {
            res.status(400).json({ error: 'Please provide a valid "task" string parameter' });
            return;
        }
        const interval = parseInt(intervalMs, 10);
        if (isNaN(interval) || interval <= 0) {
            res.status(400).json({ error: 'Please provide a valid positive integer for "intervalMs"' });
            return;
        }
        const scheduleName = name || `Schedule for ${task}`;
        const schedulePriority = parsePriority(priority);
        const schedule = await (0, db_1.insertSchedule)({
            name: scheduleName,
            taskType: task,
            intervalMs: interval,
            priority: schedulePriority,
            payload: payload || {},
        });
        res.status(201).json({
            message: 'Recurring schedule created successfully',
            schedule,
        });
    }
    catch (error) {
        console.error('Error creating schedule:', error);
        res.status(500).json({ error: 'Failed to create schedule' });
    }
});
/**
 * 10. GET /schedules
 * Fetches all recurring schedules.
 */
app.get('/schedules', async (req, res) => {
    try {
        const schedules = await (0, db_1.getSchedules)();
        res.status(200).json({
            count: schedules.length,
            schedules,
        });
    }
    catch (error) {
        console.error('Error fetching schedules:', error);
        res.status(500).json({ error: 'Failed to fetch schedules' });
    }
});
/**
 * 11. GET /schedules/:id
 * Fetches details of a specific recurring schedule.
 */
app.get('/schedules/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const schedule = await (0, db_1.getScheduleById)(id);
        if (!schedule) {
            res.status(404).json({ error: `Schedule with ID ${id} not found` });
            return;
        }
        res.status(200).json({ schedule });
    }
    catch (error) {
        console.error('Error fetching schedule:', error);
        res.status(500).json({ error: 'Failed to fetch schedule details' });
    }
});
/**
 * 12. POST /schedules/:id/pause
 * Pauses a recurring schedule.
 */
app.post('/schedules/:id/pause', async (req, res) => {
    try {
        const { id } = req.params;
        const schedule = await (0, db_1.updateScheduleState)(id, true);
        if (!schedule) {
            res.status(404).json({ error: `Schedule with ID ${id} not found` });
            return;
        }
        res.status(200).json({
            message: `Schedule ${id} paused successfully`,
            schedule,
        });
    }
    catch (error) {
        console.error('Error pausing schedule:', error);
        res.status(500).json({ error: 'Failed to pause schedule' });
    }
});
/**
 * 13. POST /schedules/:id/resume
 * Resumes a paused recurring schedule and resets next_run_at to NOW().
 */
app.post('/schedules/:id/resume', async (req, res) => {
    try {
        const { id } = req.params;
        const schedule = await (0, db_1.updateScheduleState)(id, false);
        if (!schedule) {
            res.status(404).json({ error: `Schedule with ID ${id} not found` });
            return;
        }
        res.status(200).json({
            message: `Schedule ${id} resumed successfully`,
            schedule,
        });
    }
    catch (error) {
        console.error('Error resuming schedule:', error);
        res.status(500).json({ error: 'Failed to resume schedule' });
    }
});
/**
 * 14. DELETE /schedules/:id
 * Deletes a recurring schedule.
 */
app.delete('/schedules/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const deleted = await (0, db_1.deleteSchedule)(id);
        if (!deleted) {
            res.status(404).json({ error: `Schedule with ID ${id} not found` });
            return;
        }
        res.status(200).json({
            message: `Schedule ${id} deleted successfully. Existing execution history remains intact.`,
        });
    }
    catch (error) {
        console.error('Error deleting schedule:', error);
        res.status(500).json({ error: 'Failed to delete schedule' });
    }
});
/**
 * 15. POST /jobs/:id/dependencies
 * Adds a dependency to job :id.
 * Body: { "dependsOnJobId": "123" }
 */
app.post('/jobs/:id/dependencies', async (req, res) => {
    try {
        const { id } = req.params;
        const { dependsOnJobId } = req.body;
        if (!dependsOnJobId || typeof dependsOnJobId !== 'string') {
            res.status(400).json({ error: 'Please provide a valid dependsOnJobId string parameter' });
            return;
        }
        if (id === dependsOnJobId) {
            res.status(400).json({ error: 'Self-dependency is not allowed: a job cannot depend on itself' });
            return;
        }
        const job = await (0, db_1.getJobByBullId)(id);
        if (!job) {
            res.status(404).json({ error: `Job ${id} not found in database` });
            return;
        }
        const targetJob = await (0, db_1.getJobByBullId)(dependsOnJobId);
        if (!targetJob) {
            res.status(404).json({ error: `Dependency job ${dependsOnJobId} not found in database` });
            return;
        }
        const isCircular = await (0, db_1.hasCircularDependency)(id, dependsOnJobId);
        if (isCircular) {
            res.status(400).json({
                error: `Circular dependency detected: adding dependency on job ${dependsOnJobId} creates a cycle`,
            });
            return;
        }
        const dep = await (0, db_1.addJobDependency)(id, dependsOnJobId);
        if (!dep) {
            res.status(409).json({ message: `Dependency on ${dependsOnJobId} already exists for job ${id}` });
            return;
        }
        res.status(201).json({
            message: `Dependency added successfully: Job ${id} now depends on Job ${dependsOnJobId}`,
            dependency: dep,
        });
    }
    catch (error) {
        console.error('Error adding job dependency:', error);
        res.status(500).json({ error: 'Failed to add job dependency' });
    }
});
/**
 * 16. GET /jobs/:id/dependencies
 * Returns the dependencies of job :id along with their current statuses.
 */
app.get('/jobs/:id/dependencies', async (req, res) => {
    try {
        const { id } = req.params;
        const job = await (0, db_1.getJobByBullId)(id);
        if (!job) {
            res.status(404).json({ error: `Job ${id} not found in database` });
            return;
        }
        const dependencies = await (0, db_1.getJobDependencies)(id);
        res.status(200).json({
            jobId: id,
            count: dependencies.length,
            dependencies,
        });
    }
    catch (error) {
        console.error('Error fetching job dependencies:', error);
        res.status(500).json({ error: 'Failed to fetch job dependencies' });
    }
});
/**
 * 17. DELETE /jobs/:id/dependencies/:dependencyId
 * Removes a dependency relationship for job :id.
 */
app.delete('/jobs/:id/dependencies/:dependencyId', async (req, res) => {
    try {
        const { id, dependencyId } = req.params;
        const deleted = await (0, db_1.removeJobDependency)(id, dependencyId);
        if (!deleted) {
            res.status(404).json({ error: `Dependency ${dependencyId} for job ${id} not found` });
            return;
        }
        res.status(200).json({
            message: `Dependency ${dependencyId} removed successfully from job ${id}`,
        });
    }
    catch (error) {
        console.error('Error removing job dependency:', error);
        res.status(500).json({ error: 'Failed to remove job dependency' });
    }
});
/**
 * 18. POST /workflows
 * Convenient endpoint to submit a DAG workflow of jobs with dependencies.
 * Validates against missing dependencies, self-dependencies, duplicate names, and cycles before creation.
 */
app.post('/workflows', async (req, res) => {
    try {
        const { name, jobs: workflowJobs } = req.body;
        if (!Array.isArray(workflowJobs) || workflowJobs.length === 0) {
            res.status(400).json({ error: 'Please provide a non-empty array of jobs for the workflow' });
            return;
        }
        // Validate unique job names within the workflow payload
        const jobNameSet = new Set();
        for (const j of workflowJobs) {
            if (!j.name || typeof j.name !== 'string') {
                res.status(400).json({ error: 'Each job in workflow must have a valid string name' });
                return;
            }
            if (jobNameSet.has(j.name)) {
                res.status(400).json({ error: `Duplicate job name '${j.name}' in workflow request` });
                return;
            }
            jobNameSet.add(j.name);
        }
        // Validate that all dependsOn refer to names existing in the workflow
        for (const j of workflowJobs) {
            if (Array.isArray(j.dependsOn)) {
                for (const depName of j.dependsOn) {
                    if (depName === j.name) {
                        res.status(400).json({ error: `Self-dependency detected in job '${j.name}'` });
                        return;
                    }
                    if (!jobNameSet.has(depName)) {
                        res.status(400).json({ error: `Dependency '${depName}' referenced by job '${j.name}' not found in workflow` });
                        return;
                    }
                }
            }
        }
        // Cycle detection on the requested workflow graph (Kahn's / DFS)
        const adj = new Map();
        const inDegree = new Map();
        for (const j of workflowJobs) {
            adj.set(j.name, []);
            inDegree.set(j.name, 0);
        }
        for (const j of workflowJobs) {
            if (Array.isArray(j.dependsOn)) {
                for (const depName of j.dependsOn) {
                    adj.get(depName).push(j.name);
                    inDegree.set(j.name, (inDegree.get(j.name) || 0) + 1);
                }
            }
        }
        const queue = [];
        inDegree.forEach((degree, jobName) => {
            if (degree === 0)
                queue.push(jobName);
        });
        let visitedCount = 0;
        while (queue.length > 0) {
            const current = queue.shift();
            visitedCount++;
            for (const neighbor of adj.get(current) || []) {
                inDegree.set(neighbor, inDegree.get(neighbor) - 1);
                if (inDegree.get(neighbor) === 0) {
                    queue.push(neighbor);
                }
            }
        }
        if (visitedCount < workflowJobs.length) {
            res.status(400).json({ error: 'Circular dependency detected in workflow definition' });
            return;
        }
        // Graph is a valid DAG. Create jobs and wire up dependencies.
        const createdJobsMap = new Map();
        const dependencyGraphResult = [];
        for (const j of workflowJobs) {
            const taskType = j.task || 'default-job';
            const priority = parsePriority(j.priority);
            const delay = parseDelay(j.delay);
            const payload = {
                ...(j.payload || {}),
                task: taskType,
                workflowName: name || 'Unnamed Workflow',
                jobName: j.name,
            };
            const job = await queue_1.taskQueue.add(taskType, payload, { priority, delay });
            await (0, db_1.insertJob)({
                bullmqJobId: job.id,
                taskType: job.name,
                priority: job.opts.priority ?? priority,
                maxAttempts: job.opts.attempts ?? 3,
            });
            createdJobsMap.set(j.name, job.id);
        }
        for (const j of workflowJobs) {
            const currentJobId = createdJobsMap.get(j.name);
            const dependsOnJobIds = [];
            if (Array.isArray(j.dependsOn)) {
                for (const depName of j.dependsOn) {
                    const parentJobId = createdJobsMap.get(depName);
                    await (0, db_1.addJobDependency)(currentJobId, parentJobId);
                    dependsOnJobIds.push(parentJobId);
                }
            }
            dependencyGraphResult.push({
                jobId: currentJobId,
                name: j.name,
                dependsOnJobIds,
            });
        }
        res.status(201).json({
            message: `Workflow '${name || 'Unnamed Workflow'}' created successfully with ${workflowJobs.length} jobs.`,
            workflowName: name || 'Unnamed Workflow',
            dependencyGraph: dependencyGraphResult,
        });
    }
    catch (error) {
        console.error('Error creating workflow:', error);
        res.status(500).json({ error: 'Failed to create workflow' });
    }
});
/**
 * Helper to fetch real-time worker metrics using Redis SCAN (non-blocking).
 */
async function fetchWorkerMetricsFromRedis() {
    const workerKeys = [];
    let streamCursor = '0';
    do {
        const res = await redis_1.redisClient.scan(streamCursor, 'MATCH', 'worker:heartbeat:*', 'COUNT', 100);
        streamCursor = res[0];
        workerKeys.push(...res[1]);
    } while (streamCursor !== '0');
    if (workerKeys.length === 0) {
        return [];
    }
    const rawPayloads = await redis_1.redisClient.mget(...workerKeys);
    let dbWorkerCounts = {};
    try {
        dbWorkerCounts = await (0, db_1.getWorkerJobCountsFromDb)();
    }
    catch (dbErr) {
        console.warn('[Metrics] Could not fetch worker job counts from DB:', dbErr);
    }
    const workers = [];
    const now = Date.now();
    for (const raw of rawPayloads) {
        if (!raw)
            continue;
        try {
            const data = JSON.parse(raw);
            const startTimeMs = data.startTime ? new Date(data.startTime).getTime() : now;
            const uptimeSeconds = Math.max(0, Math.floor((now - startTimeMs) / 1000));
            const pid = String(data.workerPid);
            const dbCounts = dbWorkerCounts[pid] || { completed: 0, failed: 0, running: 0 };
            workers.push({
                workerPid: pid,
                startTime: data.startTime,
                uptimeSeconds,
                concurrency: data.concurrency || 2,
                lastHeartbeat: data.lastHeartbeat,
                completedJobs: dbCounts.completed,
                failedJobs: dbCounts.failed,
                runningJobs: dbCounts.running,
            });
        }
        catch (parseErr) {
            // Ignore malformed keys
        }
    }
    return workers;
}
/**
 * Helper to fetch real-time queue metrics using BullMQ Redis APIs.
 */
async function fetchRealtimeQueueMetrics() {
    const jobCounts = await queue_1.taskQueue.getJobCounts('waiting', 'active', 'delayed', 'failed');
    const dlqJobCounts = await queue_1.dlqQueue.getJobCounts('waiting', 'active', 'completed', 'failed', 'delayed');
    const dlqCount = Object.values(dlqJobCounts).reduce((acc, count) => acc + count, 0);
    // Oldest waiting job age calculation using BullMQ getWaiting
    let oldestWaitingJobAgeMs = 0;
    try {
        const waitingJobs = await queue_1.taskQueue.getWaiting(0, 0);
        if (waitingJobs.length > 0 && waitingJobs[0]?.timestamp) {
            oldestWaitingJobAgeMs = Math.max(0, Date.now() - waitingJobs[0].timestamp);
        }
    }
    catch (err) {
        console.warn('[Metrics] Could not calculate oldest waiting job age:', err);
    }
    // Calculate throughput from DB or fallback
    let throughputJobsPerMin = 0;
    try {
        const dbJobMetrics = await (0, db_1.getJobMetricsFromDb)();
        throughputJobsPerMin = dbJobMetrics.completedLastMinute;
    }
    catch (err) {
        // If DB is offline
    }
    return {
        waiting: jobCounts.waiting || 0,
        active: jobCounts.active || 0,
        delayed: jobCounts.delayed || 0,
        failed: jobCounts.failed || 0,
        dlqCount,
        oldestWaitingJobAgeMs,
        throughputJobsPerMin,
    };
}
/**
 * 19. GET /metrics/jobs
 * Returns historical job metrics from PostgreSQL.
 */
app.get('/metrics/jobs', async (req, res) => {
    try {
        const jobMetrics = await (0, db_1.getJobMetricsFromDb)();
        res.status(200).json({
            dbConnected: true,
            metrics: jobMetrics,
        });
    }
    catch (error) {
        console.error('Error fetching job metrics:', error);
        res.status(500).json({
            dbConnected: false,
            error: 'Failed to fetch job metrics from PostgreSQL',
            message: error.message,
        });
    }
});
/**
 * 20. GET /metrics/queue
 * Returns real-time queue metrics from BullMQ and Redis.
 */
app.get('/metrics/queue', async (req, res) => {
    try {
        const queueMetrics = await fetchRealtimeQueueMetrics();
        res.status(200).json({
            metrics: queueMetrics,
        });
    }
    catch (error) {
        console.error('Error fetching queue metrics:', error);
        res.status(500).json({
            error: 'Failed to fetch queue metrics from Redis',
            message: error.message,
        });
    }
});
/**
 * 21. GET /metrics/workers
 * Returns active worker metrics using Redis SCAN and PostgreSQL job history.
 */
app.get('/metrics/workers', async (req, res) => {
    try {
        const workers = await fetchWorkerMetricsFromRedis();
        res.status(200).json({
            activeWorkersCount: workers.length,
            workers,
        });
    }
    catch (error) {
        console.error('Error fetching worker metrics:', error);
        res.status(500).json({
            error: 'Failed to fetch worker metrics from Redis',
            message: error.message,
        });
    }
});
/**
 * 22. GET /metrics/scheduler
 * Returns recurring job scheduler metrics from PostgreSQL.
 */
app.get('/metrics/scheduler', async (req, res) => {
    try {
        const schedulerMetrics = await (0, db_1.getSchedulerMetricsFromDb)();
        res.status(200).json({
            dbConnected: true,
            metrics: schedulerMetrics,
        });
    }
    catch (error) {
        console.error('Error fetching scheduler metrics:', error);
        res.status(500).json({
            dbConnected: false,
            error: 'Failed to fetch scheduler metrics from PostgreSQL',
            message: error.message,
        });
    }
});
/**
 * 23. GET /metrics/workflows
 * Returns DAG workflow metrics from PostgreSQL.
 */
app.get('/metrics/workflows', async (req, res) => {
    try {
        const workflowMetrics = await (0, db_1.getWorkflowMetricsFromDb)();
        res.status(200).json({
            dbConnected: true,
            metrics: workflowMetrics,
        });
    }
    catch (error) {
        console.error('Error fetching workflow metrics:', error);
        res.status(500).json({
            dbConnected: false,
            error: 'Failed to fetch workflow metrics from PostgreSQL',
            message: error.message,
        });
    }
});
/**
 * 23.5 GET /batches
 * Returns list of all job batches with summary information.
 */
app.get('/batches', async (req, res) => {
    try {
        const batches = await (0, db_1.getJobBatches)();
        const result = batches.map((b) => {
            const total = Number(b.total_jobs) || 0;
            const finished = (Number(b.completed_jobs) || 0) + (Number(b.failed_jobs) || 0) + (Number(b.dead_lettered_jobs) || 0);
            const progress = total > 0 ? parseFloat(((finished / total) * 100).toFixed(2)) : 0;
            return {
                id: String(b.id),
                name: b.name,
                status: b.status,
                progress,
                totalJobs: total,
                completedJobs: Number(b.completed_jobs) || 0,
                runningJobs: Number(b.running_jobs) || 0,
                failedJobs: Number(b.failed_jobs) || 0,
                deadLetteredJobs: Number(b.dead_lettered_jobs) || 0,
                createdAt: b.created_at,
                startedAt: b.started_at,
                completedAt: b.completed_at,
            };
        });
        res.status(200).json({
            count: result.length,
            batches: result,
        });
    }
    catch (error) {
        console.error('Error fetching job batches:', error);
        res.status(500).json({ error: 'Failed to fetch job batches' });
    }
});
/**
 * 23.6 GET /batches/:id
 * Returns metadata, progress, and individual jobs for a specific batch.
 */
app.get('/batches/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const batch = await (0, db_1.getJobBatchById)(id);
        if (!batch) {
            res.status(404).json({ error: `Batch with ID ${id} not found` });
            return;
        }
        const jobs = await (0, db_1.getBatchJobs)(batch.id);
        const total = Number(batch.total_jobs) || 0;
        const finished = (Number(batch.completed_jobs) || 0) + (Number(batch.failed_jobs) || 0) + (Number(batch.dead_lettered_jobs) || 0);
        const progress = total > 0 ? parseFloat(((finished / total) * 100).toFixed(2)) : 0;
        const formattedJobs = jobs.map((j) => ({
            id: String(j.id),
            bullmqJobId: j.bullmq_job_id,
            taskType: j.task_type,
            status: j.status,
            priority: j.priority,
            attempts: j.attempts,
            maxAttempts: j.max_attempts,
            workerId: j.worker_id,
            createdAt: j.created_at,
            startedAt: j.started_at,
            completedAt: j.completed_at,
            failedAt: j.failed_at,
            executionTimeMs: j.execution_time_ms,
            failedReason: j.failed_reason,
            replayedFromJobId: j.replayed_from_job_id,
        }));
        res.status(200).json({
            id: String(batch.id),
            name: batch.name,
            status: batch.status,
            progress,
            totalJobs: total,
            completedJobs: Number(batch.completed_jobs) || 0,
            runningJobs: Number(batch.running_jobs) || 0,
            failedJobs: Number(batch.failed_jobs) || 0,
            deadLetteredJobs: Number(batch.dead_lettered_jobs) || 0,
            createdAt: batch.created_at,
            startedAt: batch.started_at,
            completedAt: batch.completed_at,
            jobs: formattedJobs,
        });
    }
    catch (error) {
        console.error('Error fetching batch details:', error);
        res.status(500).json({ error: 'Failed to fetch batch details' });
    }
});
/**
 * 23.7 DELETE /batches/:id
 * Deletes batch metadata and removes batch association from jobs (preserving jobs).
 */
app.delete('/batches/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const batch = await (0, db_1.getJobBatchById)(id);
        if (!batch) {
            res.status(404).json({ error: `Batch with ID ${id} not found` });
            return;
        }
        await (0, db_1.deleteJobBatch)(id);
        res.status(200).json({
            message: `Batch ${id} deleted successfully. Associated job history preserved.`,
            batchId: String(id),
        });
    }
    catch (error) {
        console.error('Error deleting job batch:', error);
        res.status(500).json({ error: 'Failed to delete job batch' });
    }
});
/**
 * 23.8 GET /metrics/batches
 * Returns batch execution metrics from PostgreSQL.
 */
app.get('/metrics/batches', async (req, res) => {
    try {
        const batchMetrics = await (0, db_1.getBatchMetricsFromDb)();
        res.status(200).json({
            dbConnected: true,
            metrics: batchMetrics,
        });
    }
    catch (error) {
        console.error('Error fetching batch metrics:', error);
        res.status(500).json({
            dbConnected: false,
            error: 'Failed to fetch batch metrics from PostgreSQL',
            message: error.message,
        });
    }
});
/**
 * 24. GET /metrics
 * Master endpoint combining Job, Queue, Worker, Scheduler, Workflow, and Batch metrics.
 * Gracefully handles DB downtime by returning real-time Redis metrics and dbConnected: false.
 */
app.get('/metrics', async (req, res) => {
    let dbConnected = true;
    let jobMetrics = null;
    let schedulerMetrics = null;
    let workflowMetrics = null;
    let batchMetrics = null;
    try {
        jobMetrics = await (0, db_1.getJobMetricsFromDb)();
        schedulerMetrics = await (0, db_1.getSchedulerMetricsFromDb)();
        workflowMetrics = await (0, db_1.getWorkflowMetricsFromDb)();
        batchMetrics = await (0, db_1.getBatchMetricsFromDb)();
    }
    catch (dbErr) {
        console.warn('[Metrics] PostgreSQL offline or query failed, serving Redis metrics only:', dbErr);
        dbConnected = false;
    }
    let queueMetrics = null;
    let workers = [];
    try {
        queueMetrics = await fetchRealtimeQueueMetrics();
        workers = await fetchWorkerMetricsFromRedis();
    }
    catch (redisErr) {
        console.error('[Metrics] Error fetching Redis metrics:', redisErr);
    }
    res.status(200).json({
        timestamp: new Date().toISOString(),
        dbConnected,
        summary: {
            jobs: jobMetrics,
            queue: queueMetrics,
            workers: {
                activeWorkersCount: workers.length,
                workers,
            },
            scheduler: schedulerMetrics,
            workflows: workflowMetrics,
            batches: batchMetrics,
        },
    });
});
// Best-effort initialization of DB before server start
(async () => {
    await (0, db_1.initDb)();
    (0, scheduler_1.startScheduler)();
    app.listen(PORT, () => {
        console.log(`🚀 API Server running on http://localhost:${PORT}`);
    });
})();
