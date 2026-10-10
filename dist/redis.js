"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.redisClient = exports.redisConnection = void 0;
exports.createRedisClient = createRedisClient;
exports.releaseCancelLock = releaseCancelLock;
const ioredis_1 = __importDefault(require("ioredis"));
const REDIS_HOST = process.env.REDIS_HOST || '127.0.0.1';
const REDIS_PORT = parseInt(process.env.REDIS_PORT || '6379', 10);
// Shared Redis configuration connection object for BullMQ
exports.redisConnection = {
    host: REDIS_HOST,
    port: REDIS_PORT,
    // maxRetriesPerRequest must be null for BullMQ to handle connection errors cleanly
    maxRetriesPerRequest: null,
};
// Standalone ioredis client for custom Redis commands (e.g. Idempotency atomic operations)
exports.redisClient = new ioredis_1.default({
    host: REDIS_HOST,
    port: REDIS_PORT,
    maxRetriesPerRequest: null,
});
/**
 * Creates a dedicated, isolated ioredis client instance.
 * Required for Redis Pub/Sub subscriptions since a subscribing client enters subscriber mode.
 */
function createRedisClient() {
    return new ioredis_1.default({
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
async function releaseCancelLock(key, requestId) {
    try {
        const result = await exports.redisClient.eval(COMPARE_AND_DELETE_LUA, 1, key, requestId);
        return result === 1;
    }
    catch (err) {
        console.error(`[Redis] Failed to release lock ${key} for request ${requestId}:`, err);
        return false;
    }
}
