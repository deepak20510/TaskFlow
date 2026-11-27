import { redisClient } from "../redis/redisClient";

const WORKER_TTL = 15;
const WORKER_SET_KEY = "workers:active_set";

function getWorkerKey(workerId: string): string {
  return `active_worker:${workerId}`;
}

/**
 * Register worker with heartbeat TTL and membership in active set
 */
export async function registerWorker(workerId: string): Promise<void> {
  const key = getWorkerKey(workerId);

  await redisClient.set(key, "alive", {
    EX: WORKER_TTL,
  });

  await redisClient.sAdd(WORKER_SET_KEY, workerId);
  console.log(`🟢 Registered worker ${workerId}`);
}

/**
 * Renew worker heartbeat TTL
 */
export async function renewWorkerHeartbeat(workerId: string): Promise<boolean> {
  const key = getWorkerKey(workerId);
  const exists = await redisClient.exists(key);

  if (!exists) {
    // Re-register if expired
    await registerWorker(workerId);
    return true;
  }

  await redisClient.expire(key, WORKER_TTL);
  return true;
}

/**
 * Get all active workers, pruning dead workers from the set
 */
export async function getActiveWorkers(): Promise<string[]> {
  try {
    const workers = await redisClient.sMembers(WORKER_SET_KEY);
    if (!workers || workers.length === 0) {
      return [];
    }

    const activeWorkers: string[] = [];
    const deadWorkers: string[] = [];

    await Promise.all(
      workers.map(async (workerId) => {
        const isAlive = await redisClient.exists(getWorkerKey(workerId));
        if (isAlive) {
          activeWorkers.push(workerId);
        } else {
          deadWorkers.push(workerId);
        }
      }),
    );

    // Clean up dead workers asynchronously
    if (deadWorkers.length > 0) {
      await redisClient.sRem(WORKER_SET_KEY, deadWorkers);
    }

    return activeWorkers;
  } catch (err) {
    console.error("[WorkerRegistry] Error getting active workers:", err);
    return [];
  }
}

/**
 * Remove worker gracefully on shutdown
 */
export async function removeWorker(workerId: string): Promise<void> {
  const key = getWorkerKey(workerId);

  await Promise.all([
    redisClient.del(key),
    redisClient.sRem(WORKER_SET_KEY, workerId),
  ]);

  console.log(`🔴 Removed worker ${workerId}`);
}
