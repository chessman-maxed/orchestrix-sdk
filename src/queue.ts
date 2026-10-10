import { Queue } from 'bullmq';
import { redisConnection } from './redis';

// Name of our distributed job queue
export const QUEUE_NAME = 'orchestrix-queue';
export const DLQ_QUEUE_NAME = 'orchestrix-dlq';

const DEFAULT_ATTEMPTS = parseInt(process.env.JOB_MAX_ATTEMPTS || '3', 10);
const DEFAULT_BACKOFF_DELAY = parseInt(process.env.JOB_BACKOFF_DELAY_MS || '2000', 10);
export const DEFAULT_PRIORITY = parseInt(process.env.JOB_DEFAULT_PRIORITY || '5', 10);

// Instantiate BullMQ Queue to publish jobs to Redis with default retry settings and priority options
export const taskQueue = new Queue(QUEUE_NAME, {
  connection: redisConnection,
  defaultJobOptions: {
    attempts: DEFAULT_ATTEMPTS,
    backoff: {
      type: 'exponential',
      delay: DEFAULT_BACKOFF_DELAY, // Initial delay of 2000ms (2s)
    },
    priority: DEFAULT_PRIORITY,
  },
});

// Dedicated passive Dead Letter Queue for permanently failed jobs (No worker attached)
export const dlqQueue = new Queue(DLQ_QUEUE_NAME, {
  connection: redisConnection,
});



