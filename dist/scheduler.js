"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.tickScheduler = tickScheduler;
exports.startScheduler = startScheduler;
exports.stopScheduler = stopScheduler;
const queue_1 = require("./queue");
const db_1 = require("./db");
const SCHEDULER_INTERVAL_MS = parseInt(process.env.SCHEDULER_INTERVAL_MS || '1000', 10);
let schedulerTimer = null;
/**
 * Checks PostgreSQL for due schedules and enqueues corresponding jobs into BullMQ.
 * Multi-process safe: Uses atomic UPDATE ... WHERE next_run_at <= NOW() row locking.
 */
async function tickScheduler() {
    try {
        const dueSchedules = await (0, db_1.fetchDueSchedules)();
        for (const sched of dueSchedules) {
            // Atomically claim the schedule and advance next_run_at = NOW() + interval_ms
            const claimedSchedule = await (0, db_1.claimAndAdvanceSchedule)(sched.id, sched.interval_ms);
            if (!claimedSchedule) {
                // Schedule was claimed by another server instance or paused concurrently
                continue;
            }
            const taskType = claimedSchedule.task_type || 'default-job';
            const payload = {
                ...(claimedSchedule.payload || {}),
                _scheduledByScheduleId: String(claimedSchedule.id),
                _scheduledAt: new Date().toISOString(),
            };
            const priority = Number(claimedSchedule.priority) || 5;
            try {
                // Enqueue normal job to orchestrix-queue
                const job = await queue_1.taskQueue.add(taskType, payload, { priority });
                console.log(`[Scheduler] ⏰ Generated scheduled Job ID: ${job.id} for Schedule ID: ${claimedSchedule.id} ("${claimedSchedule.name}")`);
                // Best-effort insertion into PostgreSQL jobs table
                try {
                    await (0, db_1.insertJob)({
                        bullmqJobId: job.id,
                        taskType: job.name,
                        priority: job.opts.priority ?? priority,
                        maxAttempts: job.opts.attempts ?? 3,
                    });
                }
                catch (dbError) {
                    console.error(`[DB] ❌ Failed to insert scheduled job ${job.id} into PostgreSQL jobs table:`, dbError);
                }
            }
            catch (enqueueError) {
                // Explicit error handling when taskQueue.add() fails
                console.error(`[Scheduler] ❌ ENQUEUE ERROR: Failed to add job to orchestrix-queue for Schedule ID ${claimedSchedule.id} ("${claimedSchedule.name}"):`, enqueueError);
            }
        }
    }
    catch (error) {
        console.error('[Scheduler] ❌ Error running scheduler tick:', error);
    }
}
/**
 * Starts the background polling loop for job scheduling.
 */
function startScheduler() {
    if (schedulerTimer)
        return;
    console.log(`⏰ Starting Scheduler Engine (Interval: ${SCHEDULER_INTERVAL_MS}ms)`);
    schedulerTimer = setInterval(tickScheduler, SCHEDULER_INTERVAL_MS);
}
/**
 * Stops the scheduler background timer.
 */
function stopScheduler() {
    if (schedulerTimer) {
        clearInterval(schedulerTimer);
        schedulerTimer = null;
        console.log('⏰ Scheduler Engine stopped');
    }
}
