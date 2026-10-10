import { Worker, Job, UnrecoverableError } from 'bullmq';
import { redisConnection, redisClient, createRedisClient } from './redis';
import { QUEUE_NAME, dlqQueue } from './queue';
import {
  initDb,
  updateJobStarted,
  updateJobCompleted,
  updateJobFailed,
  updateJobStalled,
  updateJobDeadLettered,
  evaluateJobDependencies,
  updateBatchProgress,
  getJobByBullId,
  cancelJobInDb,
} from './db';

export class JobCancelledError extends Error {
  constructor(reason?: string) {
    super(reason || 'Job cancelled by user');
    this.name = 'JobCancelledError';
  }
}

/**
 * Cancellable delay function that rejects immediately with JobCancelledError if AbortSignal fires.
 */
export function cancellableDelay(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      return reject(new JobCancelledError('Job cancelled by user'));
    }

    const onAbort = () => {
      cleanup();
      reject(new JobCancelledError('Job cancelled by user'));
    };

    const timer = setTimeout(() => {
      cleanup();
      resolve();
    }, ms);

    const cleanup = () => {
      clearTimeout(timer);
      signal?.removeEventListener('abort', onAbort);
    };

    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

export type ActiveJobState = 'executing' | 'completing' | 'cancelling' | 'settled';

export interface ActiveJobContext {
  job: Job;
  abortController: AbortController;
  state: ActiveJobState;
  cancelRequest?: {
    requestId: string;
    cancelledBy?: string;
    reason?: string;
  };
  executionId: string;
}

const activeJobs = new Map<string, ActiveJobContext>();

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

const workerPid = process.pid;
const workerStartTime = new Date().toISOString();
const concurrency = parseInt(process.env.WORKER_CONCURRENCY || '2', 10);
const lockDuration = parseInt(process.env.WORKER_LOCK_DURATION || '5000', 10);
const stalledInterval = parseInt(process.env.WORKER_STALLED_INTERVAL || '5000', 10);
const maxStalledCount = parseInt(process.env.WORKER_MAX_STALLED_COUNT || '1', 10);

const HEARTBEAT_KEY = `worker:heartbeat:${workerPid}`;
const HEARTBEAT_INTERVAL_MS = 5000;
const HEARTBEAT_TTL_SECONDS = 15;

async function sendHeartbeat(): Promise<void> {
  try {
    const payload = JSON.stringify({
      workerPid: String(workerPid),
      startTime: workerStartTime,
      concurrency,
      lastHeartbeat: new Date().toISOString(),
    });
    await redisClient.set(HEARTBEAT_KEY, payload, 'EX', HEARTBEAT_TTL_SECONDS);
  } catch (err) {
    console.error(`[Worker ${workerPid}] ⚠️ Failed to send heartbeat to Redis:`, err);
  }
}

let heartbeatTimer: NodeJS.Timeout | null = null;

async function cleanupWorker(): Promise<void> {
  if (heartbeatTimer) {
    clearInterval(heartbeatTimer);
    heartbeatTimer = null;
  }
  try {
    await redisClient.del(HEARTBEAT_KEY);
    console.log(`[Worker ${workerPid}] 🛑 Cleaned up worker heartbeat key on shutdown.`);
  } catch (err) {
    console.error(`[Worker ${workerPid}] Error cleaning up heartbeat key:`, err);
  }
}

process.on('SIGINT', async () => {
  await cleanupWorker();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  await cleanupWorker();
  process.exit(0);
});

(async () => {
  console.log(
    `⚡ Starting Worker Process [PID: ${workerPid}] | Concurrency: ${concurrency} | LockDuration: ${lockDuration}ms | StalledInterval: ${stalledInterval}ms | MaxStalledCount: ${maxStalledCount}`
  );

  await initDb();
  await sendHeartbeat();
  heartbeatTimer = setInterval(sendHeartbeat, HEARTBEAT_INTERVAL_MS);

  // Dedicated Redis Subscriber client listening for job cancellation requests
  const cancelSubscriber = createRedisClient();
  try {
    await cancelSubscriber.subscribe('channel:job:cancel:req');
    cancelSubscriber.on('message', async (channel, messageStr) => {
      if (channel !== 'channel:job:cancel:req') return;
      try {
        const msg = JSON.parse(messageStr);
        const { requestId, jobId, cancelledBy, reason } = msg;
        if (!jobId || !requestId) return;

        const context = activeJobs.get(String(jobId));
        if (!context) return;

        const transitioned = tryTransitionJobState(context, 'executing', 'cancelling');
        if (transitioned) {
          context.cancelRequest = { requestId, cancelledBy, reason };
          // Publish RECEIVED ACK immediately upon claiming active job cancellation
          await redisClient.publish(
            `channel:job:cancel:resp:${requestId}`,
            JSON.stringify({
              requestId,
              jobId: String(jobId),
              workerPid,
              status: 'RECEIVED',
            })
          );
          context.abortController.abort();
        } else if (context.state === 'completing' || context.state === 'settled') {
          await redisClient.publish(
            `channel:job:cancel:resp:${requestId}`,
            JSON.stringify({
              requestId,
              jobId: String(jobId),
              workerPid,
              status: 'RACE_LOST',
              finalStatus: 'completed',
            })
          );
        }
      } catch (err) {
        console.error(`[Worker ${workerPid}] Error processing cancel request message:`, err);
      }
    });
  } catch (subErr) {
    console.error(`[Worker ${workerPid}] ⚠️ Failed to initialize cancel subscriber:`, subErr);
  }

  // Create a worker that listens on QUEUE_NAME
  const worker = new Worker(
    QUEUE_NAME,
    async (job: Job) => {
      const startTime = Date.now();
      const attemptNum = job.attemptsMade + 1; // 1-indexed current attempt number
      const maxAttempts = job.opts.attempts || 1;
      const priority = job.opts.priority || 5;

      const abortController = new AbortController();
      const executionId = `${job.id}:${attemptNum}:${job.token || ''}`;

      const context: ActiveJobContext = {
        job,
        abortController,
        state: 'executing',
        executionId,
      };
      activeJobs.set(String(job.id!), context);

      try {
        console.log(
          `[Worker ${workerPid}] 🚀 Started processing check for Job ID: ${job.id} (Priority: ${priority}, Attempt ${attemptNum} of ${maxAttempts})`
        );
        console.log(`[Worker ${workerPid}] Job Payload:`, JSON.stringify(job.data));

        // Pre-execution cancellation guard: verify PostgreSQL status before executing work
        try {
          const dbJob = await getJobByBullId(job.id!);
          if (dbJob && dbJob.status === 'cancelled') {
            console.warn(`[Worker ${workerPid}] 🛑 Job ID ${job.id} was cancelled before execution. Skipping processing.`);
            return { status: 'cancelled_before_execution', jobId: job.id };
          }
        } catch (checkErr) {
          console.warn(`[Worker ${workerPid}] Warning checking DB status for job ${job.id}:`, checkErr);
        }

        // Dependency pre-check
        try {
          const depEval = await evaluateJobDependencies(job.id!);
          if (depEval.hasFailed) {
            console.warn(
              `[Worker ${workerPid}] ⛔ Job ID ${job.id} blocked! Dependency job ${depEval.failedDependencyId} permanently failed or dead-lettered.`
            );
            throw new Error(
              `Dependency failure: Job ${job.id} blocked because dependency job ${depEval.failedDependencyId} failed.`
            );
          }

          if (!depEval.allCompleted) {
            console.log(
              `[Worker ${workerPid}] ⏳ Job ID ${job.id} waiting for ${depEval.pendingCount} incomplete dependencies. Deferring execution for 2000ms...`
            );
            await cancellableDelay(2000, abortController.signal);
            throw new Error(
              `Dependencies incomplete: Job ${job.id} waiting for ${depEval.pendingCount} dependencies to complete.`
            );
          }
        } catch (depErr: any) {
          throw depErr;
        }

        // Mark job as running in PostgreSQL
        try {
          const batchId = await updateJobStarted(job.id!, String(workerPid));
          if (batchId) {
            await updateBatchProgress(batchId);
          }
        } catch (dbError) {
          console.error(`[DB] ❌ Failed to update job ${job.id} to 'running' in PostgreSQL:`, dbError);
        }

        // Simulate intentional crash if requested in payload or job name
        if (job.name === 'simulate-crash' || (job.data && job.data.task === 'simulate-crash')) {
          console.warn(
            `[Worker ${workerPid}] 💥 Simulating abrupt WORKER PROCESS CRASH for Job ID: ${job.id} on Attempt ${attemptNum}`
          );
          await cancellableDelay(1000, abortController.signal);
          process.exit(1);
        }

        // Simulate intentional failure if requested in payload or job name
        if (job.name === 'simulate-failure' || (job.data && job.data.task === 'simulate-failure')) {
          console.warn(
            `[Worker ${workerPid}] ⚠️ Simulating intentional failure for Job ID: ${job.id} on Attempt ${attemptNum}`
          );
          throw new Error(`Simulated failure on attempt ${attemptNum} of ${maxAttempts}`);
        }

        // Simulate work execution with cooperative cancellation support
        if (job.name === 'simulate-long-job' || (job.data && job.data.task === 'simulate-long-job')) {
          console.log(
            `[Worker ${workerPid}] ⏳ Started long-running test job for Job ID: ${job.id} (Executing for 60s)...`
          );
          await cancellableDelay(60000, abortController.signal);
        } else {
          // Simulate standard 3-second job
          await cancellableDelay(3000, abortController.signal);
        }

        const executionTimeMs = Date.now() - startTime;

        // Attempt normal completion claim
        const transitionedToCompleting = tryTransitionJobState(context, 'executing', 'completing');
        if (!transitionedToCompleting) {
          // State was changed to 'cancelling' concurrently
          throw new JobCancelledError();
        }

        // Normal completion PostgreSQL update
        try {
          const { rowCount, batchId } = await updateJobCompleted({
            bullmqJobId: job.id!,
            attemptsMade: job.attemptsMade,
            executionTimeMs,
          });

          if (rowCount === 1) {
            if (batchId) {
              await updateBatchProgress(batchId);
            }
            if (context.cancelRequest) {
              await redisClient.publish(
                `channel:job:cancel:resp:${context.cancelRequest.requestId}`,
                JSON.stringify({
                  requestId: context.cancelRequest.requestId,
                  jobId: String(job.id!),
                  workerPid,
                  status: 'RACE_LOST',
                  finalStatus: 'completed',
                })
              );
            }
          } else {
            const currentDbJob = await getJobByBullId(job.id!);
            if (currentDbJob?.status === 'cancelled') {
              console.log(`[Worker ${workerPid}] 🛑 Completion skipped for ${job.id}: Job was cancelled in DB.`);
              throw new UnrecoverableError('Job cancelled by user');
            }
          }
        } catch (dbCompErr: any) {
          if (dbCompErr instanceof UnrecoverableError) throw dbCompErr;
          console.error(`[DB] ❌ Failed to update completed job ${job.id} in PostgreSQL:`, dbCompErr);
          throw dbCompErr;
        }

        console.log(`[Worker ${workerPid}] ✅ Completed Job ID: ${job.id} in ${executionTimeMs}ms`);

        return {
          result: 'Job processed successfully',
          workerPid,
          executionTimeMs,
          processedAt: new Date().toISOString(),
        };
      } catch (err: any) {
        if (
          err?.name === 'JobCancelledError' ||
          abortController.signal.aborted ||
          context.state === 'cancelling'
        ) {
          console.warn(`[Worker ${workerPid}] 🛑 Cooperative task execution unrolled for Job ID ${job.id}. Cleaning up...`);

          // Store persistent durable intent marker in Redis before DB update
          try {
            await redisClient.set(
              `job:cancel:durable:${job.id}`,
              JSON.stringify({
                jobId: String(job.id!),
                executionId,
                cancelledBy: context.cancelRequest?.cancelledBy || 'user',
                reason: context.cancelRequest?.reason || 'Cancelled mid-execution',
                stoppedAt: new Date().toISOString(),
                workerPid,
              })
            );
          } catch (redisErr) {
            console.error(`[Redis] Failed to write durable cancellation marker for job ${job.id}:`, redisErr);
          }

          // Atomic conditional transition in PostgreSQL
          let rowCount = 0;
          let batchId: number | null = null;
          let currentStatus: string | null = null;

          try {
            const dbResult = await cancelJobInDb({
              bullmqJobId: job.id!,
              cancelledBy: context.cancelRequest?.cancelledBy,
              reason: context.cancelRequest?.reason,
            });
            rowCount = dbResult.rowCount;
            batchId = dbResult.batchId;
            currentStatus = dbResult.status;
          } catch (dbCancelErr) {
            console.error(`[DB] ❌ Error executing cancelJobInDb for job ${job.id}:`, dbCancelErr);
          }

          if (rowCount === 1) {
            if (batchId) {
              try {
                await updateBatchProgress(batchId);
              } catch (bErr) {
                console.error(`[DB] Error updating batch progress for batch ${batchId}:`, bErr);
              }
            }

            // Remove durable marker upon confirmed PostgreSQL cancellation commit
            try {
              await redisClient.del(`job:cancel:durable:${job.id}`);
            } catch (delErr) {
              console.error(`[Redis] Error removing durable cancellation marker for job ${job.id}:`, delErr);
            }

            if (context.cancelRequest) {
              try {
                await redisClient.publish(
                  `channel:job:cancel:resp:${context.cancelRequest.requestId}`,
                  JSON.stringify({
                    requestId: context.cancelRequest.requestId,
                    jobId: String(job.id!),
                    workerPid,
                    status: 'STOPPED',
                    finalStatus: 'cancelled',
                    batchId,
                  })
                );
              } catch (pubErr) {
                console.error(`[Redis] Failed to publish STOPPED ACK for job ${job.id}:`, pubErr);
              }
            }
          } else {
            // Row count === 0: verify actual status in DB
            try {
              const actualDbJob = await getJobByBullId(job.id!);
              const actualStatus = actualDbJob?.status || currentStatus || 'unknown';

              if (actualStatus === 'completed' || actualStatus === 'failed' || actualStatus === 'dead_lettered') {
                await redisClient.del(`job:cancel:durable:${job.id}`);
              }

              if (context.cancelRequest) {
                await redisClient.publish(
                  `channel:job:cancel:resp:${context.cancelRequest.requestId}`,
                  JSON.stringify({
                    requestId: context.cancelRequest.requestId,
                    jobId: String(job.id!),
                    workerPid,
                    status: actualStatus === 'cancelled' ? 'STOPPED' : 'RACE_LOST',
                    finalStatus: actualStatus,
                  })
                );
              }
            } catch (verErr) {
              console.error(`[DB] Error verifying actual status after zero rowCount for job ${job.id}:`, verErr);
            }
          }

          // Throw UnrecoverableError to stop BullMQ retries cleanly
          throw new UnrecoverableError('Job cancelled by user');
        }

        throw err;
      } finally {
        context.state = 'settled';
        activeJobs.delete(String(job.id!));
      }
    },
    {
      connection: redisConnection,
      concurrency,
      lockDuration,
      stalledInterval,
      maxStalledCount,
    }
  );

  // Worker Lifecycle Event Listeners
  worker.on('completed', async (job) => {
    if (job.returnvalue?.status === 'cancelled_before_execution') {
      return;
    }
    console.log(`[Worker ${workerPid}] 🎉 [Event] Job ${job.id} status updated to COMPLETED`);
  });

  worker.on('failed', async (job, err) => {
    if (
      err?.name === 'JobCancelledError' ||
      err?.message?.includes('Job cancelled') ||
      (err instanceof UnrecoverableError && err.message.includes('cancelled'))
    ) {
      console.log(`[Worker ${workerPid}] 🛑 [Event] Job ${job?.id} cancelled by user. Skipping retries, failure update, and DLQ.`);
      return;
    }

    const maxAttempts = job?.opts.attempts || 1;
    const attemptsMade = job?.attemptsMade || 0;
    const isPermanent = attemptsMade >= maxAttempts;

    if (!isPermanent) {
      console.warn(
        `[Worker ${workerPid}] ⏳ [Event] Job ${job?.id} failed attempt ${attemptsMade}/${maxAttempts}. BullMQ scheduling retry with exponential backoff... Error: ${err.message}`
      );
    } else {
      console.error(
        `[Worker ${workerPid}] ❌ [Event] Job ${job?.id} PERMANENTLY FAILED after all ${maxAttempts} attempts exhausted! Error: ${err.message}`
      );
    }

    if (job?.id) {
      try {
        const { batchId } = await updateJobFailed({
          bullmqJobId: job.id,
          attemptsMade,
          maxAttempts,
          failedReason: err.message,
        });
        if (isPermanent && batchId) {
          await updateBatchProgress(batchId);
        }
      } catch (dbError) {
        console.error(`[DB] ❌ Failed to update failed job ${job.id} in PostgreSQL:`, dbError);
      }

      if (isPermanent) {
        const dlqJobId = `dlq-${job.id}`;
        let dlqSuccess = false;

        try {
          await dlqQueue.add(
            'dlq-snapshot',
            {
              originalJobId: job.id,
              taskType: job.name,
              payload: job.data,
              failedReason: err.message,
              attemptsMade,
              maxAttempts,
              priority: job.opts.priority || 5,
              failedAt: new Date().toISOString(),
            },
            { jobId: dlqJobId }
          );
          dlqSuccess = true;
        } catch (dlqErr: any) {
          try {
            const existingDlqJob = await dlqQueue.getJob(dlqJobId);
            if (existingDlqJob) {
              dlqSuccess = true;
            } else {
              console.error(`[DLQ] ❌ Failed to insert job ${job.id} into DLQ (snapshot missing):`, dlqErr);
            }
          } catch (checkErr) {
            console.error(`[DLQ] ❌ Failed to insert job ${job.id} into DLQ and verify existence:`, checkErr);
          }
        }

        if (dlqSuccess) {
          try {
            const batchId = await updateJobDeadLettered({
              bullmqJobId: job.id,
              attemptsMade,
              failedReason: err.message,
            });
            if (batchId) {
              await updateBatchProgress(batchId);
            }
            console.log(`[DLQ] 📥 Job ${job.id} successfully snapshot to DLQ and marked 'dead_lettered' in DB`);
          } catch (dbSyncErr) {
            console.error(
              `[DB] ❌ DLQ snapshot succeeded for job ${job.id}, but PostgreSQL status update to 'dead_lettered' failed:`,
              dbSyncErr
            );
          }
        }
      }
    }
  });

  worker.on('stalled', async (jobId, prev) => {
    console.warn(
      `[Worker ${workerPid}] ⚠️ [Event] Stalled job detected! Job ID ${jobId} (previous state: ${prev}) was left active by a crashed worker and has been recovered for re-execution.`
    );

    try {
      const batchId = await updateJobStalled(jobId);
      if (batchId) {
        await updateBatchProgress(batchId);
      }
    } catch (dbError) {
      console.error(`[DB] ❌ Failed to update stalled job ${jobId} in PostgreSQL:`, dbError);
    }
  });
})();
