import { ConnectionOptions } from 'bullmq';
import Redis from 'ioredis';

const REDIS_HOST = process.env.REDIS_HOST || '127.0.0.1';
const REDIS_PORT = parseInt(process.env.REDIS_PORT || '6379', 10);

// Shared Redis configuration connection object for BullMQ
export const redisConnection: ConnectionOptions = {
  host: REDIS_HOST,
  port: REDIS_PORT,
  // maxRetriesPerRequest must be null for BullMQ to handle connection errors cleanly
  maxRetriesPerRequest: null,
};

// Standalone ioredis client for custom Redis commands (e.g. Idempotency atomic operations)
export const redisClient = new Redis({
  host: REDIS_HOST,
  port: REDIS_PORT,
  maxRetriesPerRequest: null,
});

/**
 * Creates a dedicated, isolated ioredis client instance.
 * Required for Redis Pub/Sub subscriptions since a subscribing client enters subscriber mode.
 */
export function createRedisClient(): Redis {
  return new Redis({
    host: REDIS_HOST,
    port: REDIS_PORT,
    maxRetriesPerRequest: null,
  });
}

const COMPARE_AND_DELETE_LUA = `
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
else
  return 0
end
`;

/**
 * Atomically releases a single-flight cancellation lock ONLY if its stored request ID matches.
 */
export async function releaseCancelLock(key: string, requestId: string): Promise<boolean> {
  try {
    const result = await redisClient.eval(COMPARE_AND_DELETE_LUA, 1, key, requestId);
    return result === 1;
  } catch (err) {
    console.error(`[Redis] Failed to release lock ${key} for request ${requestId}:`, err);
    return false;
  }
}


