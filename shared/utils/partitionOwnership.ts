import { redisClient } from "../redis/redisClient";
import {
  compareAndDeleteScript,
  compareAndExpireScript,
} from "../redis/luaScripts";

const LEASE_TTL = 30;

function getPartitionOwnerKey(queue: string): string {
  return `partition_owner:${queue}`;
}

/**
 * Try claiming partition ownership atomically
 */
export async function claimPartition(
  queue: string,
  workerId: string,
  ttlSeconds: number = LEASE_TTL,
): Promise<boolean> {
  try {
    const key = getPartitionOwnerKey(queue);

    const result = await redisClient.set(key, workerId, {
      NX: true,
      EX: ttlSeconds,
    });

    return result === "OK";
  } catch (error) {
    console.error("[PartitionOwnership] Claim partition error:", error);
    return false;
  }
}

/**
 * Renew lease atomically if and only if the current worker is the verified owner
 */
export async function renewPartitionLease(
  queue: string,
  workerId: string,
  ttlSeconds: number = LEASE_TTL,
): Promise<boolean> {
  try {
    const key = getPartitionOwnerKey(queue);

    const result = await redisClient.eval(compareAndExpireScript, {
      keys: [key],
      arguments: [workerId, ttlSeconds.toString()],
    });

    return result === 1;
  } catch (error) {
    console.error("[PartitionOwnership] renewPartitionLease error:", error);
    return false;
  }
}

/**
 * Release ownership gracefully using atomic compare-and-delete Lua script
 * to prevent releasing another worker's lease if this worker's lease expired.
 */
export async function releasePartition(
  queue: string,
  workerId: string,
): Promise<boolean> {
  try {
    const key = getPartitionOwnerKey(queue);

    const result = await redisClient.eval(compareAndDeleteScript, {
      keys: [key],
      arguments: [workerId],
    });

    const released = result === 1;
    if (released) {
      console.log(`🔓 Released ${queue} by ${workerId}`);
    }
    return released;
  } catch (error) {
    console.error("[PartitionOwnership] Release partition error:", error);
    return false;
  }
}

/**
 * Query current owner of a partition
 */
export async function getPartitionOwner(queue: string): Promise<string | null> {
  try {
    const key = getPartitionOwnerKey(queue);
    return await redisClient.get(key);
  } catch (error) {
    console.error("[PartitionOwnership] getPartitionOwner error:", error);
    return null;
  }
}
