"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.dlqQueue = exports.taskQueue = exports.DEFAULT_PRIORITY = exports.DLQ_QUEUE_NAME = exports.QUEUE_NAME = void 0;
const bullmq_1 = require("bullmq");
const redis_1 = require("./redis");
// Name of our distributed job queue
exports.QUEUE_NAME = 'orchestrix-queue';
exports.DLQ_QUEUE_NAME = 'orchestrix-dlq';
const DEFAULT_ATTEMPTS = parseInt(process.env.JOB_MAX_ATTEMPTS || '3', 10);
const DEFAULT_BACKOFF_DELAY = parseInt(process.env.JOB_BACKOFF_DELAY_MS || '2000', 10);
exports.DEFAULT_PRIORITY = parseInt(process.env.JOB_DEFAULT_PRIORITY || '5', 10);
// Instantiate BullMQ Queue to publish jobs to Redis with default retry settings and priority options
exports.taskQueue = new bullmq_1.Queue(exports.QUEUE_NAME, {
    connection: redis_1.redisConnection,
    defaultJobOptions: {
        attempts: DEFAULT_ATTEMPTS,
        backoff: {
            type: 'exponential',
            delay: DEFAULT_BACKOFF_DELAY, // Initial delay of 2000ms (2s)
        },
        priority: exports.DEFAULT_PRIORITY,
    },
});
// Dedicated passive Dead Letter Queue for permanently failed jobs (No worker attached)
exports.dlqQueue = new bullmq_1.Queue(exports.DLQ_QUEUE_NAME, {
    connection: redis_1.redisConnection,
});
