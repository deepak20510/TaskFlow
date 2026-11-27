import crypto from "crypto";
import "dotenv/config";
import { connectRedis, redisClient } from "../../shared/redis/redisClient";
import { processJob } from "../processors/jobProcessor";
import { metrics } from "../../shared/metrics/metrics";
import {
  jobDuration,
  dbDuration,
  businessLogicDuration,
} from "../../shared/metrics/prometheus";
import {
  claimPartition,
  releasePartition,
  renewPartitionLease,
} from "../../shared/utils/partitionOwnership";
import {
  getActiveWorkers,
  registerWorker,
  removeWorker,
  renewWorkerHeartbeat,
} from "../../shared/cluster/workerRegistry";
import { Scheduler } from "../scheduler/schedular";
import { startMetricsServer } from "../../shared/metrics/metricsServer";
import { RedisStreamQueue } from "../../shared/redis/streamQueue";
import { DistributedLockManager, LockHandle } from "../../shared/redis/lockManager";
import { JobStateService } from "../../shared/lifecycle/jobStateService";
import { calculateBackoffDelay } from "../../shared/utils/backoff";
import { DelayedQueueManager } from "../../shared/delayed/delayedQueue";
import { isNonRetryableError } from "../../shared/errors/jobErrors";
import { getAllStreams } from "../../shared/utils/partition";

console.log("🚀 WORKER FILE STARTED");
const workerId = `worker-${crypto.randomUUID()}`;

let heartbeatInterval: NodeJS.Timeout | null = null;
let delayedQueueInterval: NodeJS.Timeout | null = null;
let isShuttingDown = false;
let activeJobsCount = 0;

const ALL_PARTITIONS = [
  // High
  "high_priority_stream:0",
  "high_priority_stream:1",
  "high_priority_stream:2",
  "high_priority_stream:3",

  // Medium
  "medium_priority_stream:0",
  "medium_priority_stream:1",
  "medium_priority_stream:2",
  "medium_priority_stream:3",

  // Low
  "low_priority_stream:0",
  "low_priority_stream:1",
  "low_priority_stream:2",
  "low_priority_stream:3",
];

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Graceful shutdown sequence
 */
async function shutdown(): Promise<void> {
  if (isShuttingDown) return;
  isShuttingDown = true;

  console.log(`🛑 [Worker] Initiating graceful shutdown for ${workerId}...`);

  if (heartbeatInterval) clearInterval(heartbeatInterval);
  if (delayedQueueInterval) clearInterval(delayedQueueInterval);

  // Wait for in-flight jobs to finish with a grace period
  const GRACE_PERIOD_MS = 15000;
  const shutdownStart = Date.now();

  while (activeJobsCount > 0 && Date.now() - shutdownStart < GRACE_PERIOD_MS) {
    console.log(`⏳ [Worker] Waiting for ${activeJobsCount} in-flight job(s) to complete...`);
    await sleep(500);
  }

  // Release all partition ownerships
  for (const queue of ALL_PARTITIONS) {
    try {
      await releasePartition(queue, workerId);
    } catch {}
  }

  // Remove worker registration
  try {
    await removeWorker(workerId);
  } catch {}

  console.log(`✅ [Worker] Clean shutdown complete for ${workerId}.`);
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

async function startHeartbeat(
  ownedPartitions: Set<string>,
  workerId: string,
  scheduler: Scheduler,
) {
  while (!isShuttingDown) {
    try {
      for (const queue of [...ownedPartitions]) {
        const renewed = await renewPartitionLease(queue, workerId);
        if (!renewed) {
          console.warn(`[Worker] Lost ownership of ${queue}`);
          ownedPartitions.delete(queue);
          scheduler.updateQueues(Array.from(ownedPartitions));
        }
      }
    } catch (err) {
      console.error("[Worker] Heartbeat loop error:", err);
    }
    await sleep(5000);
  }
}

async function startClaimLoop(
  ownedPartitions: Set<string>,
  workerId: string,
  scheduler: Scheduler,
) {
  while (!isShuttingDown) {
    try {
      const workers = await getActiveWorkers();
      if (workers.length === 0) {
        await sleep(1000);
        continue;
      }
      const fairShare = Math.ceil(ALL_PARTITIONS.length / workers.length);

      for (const queue of ALL_PARTITIONS) {
        if (ownedPartitions.has(queue)) continue;
        if (ownedPartitions.size >= fairShare) break;

        const claimed = await claimPartition(queue, workerId);
        if (claimed) {
          ownedPartitions.add(queue);
          scheduler.updateQueues(Array.from(ownedPartitions));
          console.log(`🟢 ${workerId} claimed ${queue}`);
        }
      }
    } catch (err) {
      console.error("[Worker] Claim loop error:", err);
    }
    await sleep(5000);
  }
}

async function startRebalance(
  workerId: string,
  ownedPartitions: Set<string>,
  scheduler: Scheduler,
) {
  while (!isShuttingDown) {
    try {
      const workers = await getActiveWorkers();
      if (workers.length === 0) {
        await sleep(5000);
        continue;
      }
      const fairShare = Math.ceil(ALL_PARTITIONS.length / workers.length);
      const maxAllowed = fairShare + 1;

      if (ownedPartitions.size > maxAllowed) {
        const partitionToRelease = [...ownedPartitions][0];
        console.log(`⚖️ ${workerId} releasing ${partitionToRelease} for rebalance`);
        await releasePartition(partitionToRelease, workerId);
        ownedPartitions.delete(partitionToRelease);
        scheduler.updateQueues(Array.from(ownedPartitions));
      }
    } catch (error) {
      console.error("[Worker] Rebalance loop error:", error);
    }
    await sleep(10000);
  }
}

export async function startWorker(): Promise<void> {
  console.log("Starting worker engine...");

  await connectRedis();
  console.log("Worker connected to Redis");

  await registerWorker(workerId);
  heartbeatInterval = setInterval(async () => {
    if (isShuttingDown) return;
    try {
      await renewWorkerHeartbeat(workerId);
    } catch (err) {
      console.error("[Worker] Worker heartbeat failed:", err);
    }
  }, 5000);

  // Background processor for delayed retries
  delayedQueueInterval = setInterval(async () => {
    if (isShuttingDown) return;
    try {
      await DelayedQueueManager.processReadyJobs(20);
    } catch (err) {
      console.error("[Worker] Delayed queue processor error:", err);
    }
  }, 1000);

  const ownedPartitions = new Set<string>();
  const scheduler = new Scheduler();

  void startHeartbeat(ownedPartitions, workerId, scheduler);
  void startClaimLoop(ownedPartitions, workerId, scheduler);
  void startRebalance(workerId, ownedPartitions, scheduler);

  // Main consumer loop
  while (!isShuttingDown) {
    let streamMessage: any = null;
    let endJobTimer: (() => void) | undefined;
    let lockHandle: LockHandle | null = null;

    try {
      const queue = scheduler.nextQueue();

      if (!queue) {
        await sleep(50);
        continue;
      }

      // Read next message immediately (non-blocking)
      streamMessage = await RedisStreamQueue.readNextJob(queue, workerId, undefined, 0);

      // If no new messages, check for abandoned jobs in stream
      if (!streamMessage) {
        streamMessage = await RedisStreamQueue.claimAbandonedJob(queue, workerId, undefined, 30000);
      }

      scheduler.recordResult(queue, !!streamMessage);

      if (!streamMessage) {
        continue;
      }

      activeJobsCount++;
      const job = streamMessage.data;
      const totalStart = performance.now();

      console.log(`📥 Job received: ${job.id} (type: ${job.type}, priority: ${job.priority})`);

      // ============================================================
      // STEP 1: CONDITIONAL DB TRANSITION TO PROCESSING
      // ============================================================
      const endDbProcessing = dbDuration.startTimer({ operation: "update_processing" });
      let claimedJob: any = null;

      try {
        claimedJob = await JobStateService.transitionToProcessing(job.id, workerId);
      } finally {
        endDbProcessing();
      }

      if (!claimedJob) {
        console.log(`⚡ Job ${job.id} already processed or claimed by another worker. Acknowledging.`);
        await RedisStreamQueue.acknowledgeAndClean(streamMessage.streamName, streamMessage.streamMessageId);
        activeJobsCount--;
        continue;
      }

      await metrics.incrementProcessing(job.type);

      // ============================================================
      // STEP 2: DISTRIBUTED LOCK WITH AUTOMATIC HEARTBEAT
      // ============================================================
      const lockKey = `lock:${job.id}`;
      const uniqueToken = `${workerId}:${crypto.randomUUID()}`;

      lockHandle = await DistributedLockManager.acquireLockWithHeartbeat(lockKey, 30, uniqueToken);

      if (!lockHandle.acquired) {
        console.warn(`🔒 Job already locked by another worker: ${job.id}. Reverting DB state.`);
        await JobStateService.revertToQueued(job.id, workerId);
        activeJobsCount--;
        continue;
      }

      // ============================================================
      // STEP 3: IDEMPOTENCY CHECK (FAST PATH VIA REDIS)
      // ============================================================
      const isAlreadyProcessed = await redisClient.exists(`processed:${job.id}`);
      if (isAlreadyProcessed) {
        console.log(`⚡ Skipping job (fast-path idempotency): ${job.id}`);
        await RedisStreamQueue.acknowledgeAndClean(streamMessage.streamName, streamMessage.streamMessageId);
        await lockHandle.release();
        activeJobsCount--;
        continue;
      }

      // ============================================================
      // STEP 4: EXECUTE BUSINESS LOGIC
      // ============================================================
      endJobTimer = jobDuration.startTimer();
      const processStart = performance.now();
      const endBusinessTimer = businessLogicDuration.startTimer();

      try {
        await processJob(job);
      } finally {
        endBusinessTimer();
      }

      const duration = performance.now() - processStart;

      // ============================================================
      // STEP 5: MARK COMPLETED IN DB & METRICS
      // ============================================================
      const endDbCompleted = dbDuration.startTimer({ operation: "update_completed" });
      try {
        await JobStateService.transitionToCompleted(job.id, workerId);
      } finally {
        endDbCompleted();
      }

      try {
        await metrics.incrementProcessed(job.type);
        await metrics.decrementProcessing(job.type);
        await metrics.recordProcessingTime(job.type, duration);
      } catch (e) {
        console.error("[Worker] Metrics error:", e);
      }

      // Mark processed in Redis cache for 1 hour to optimize duplicates
      await redisClient.set(`processed:${job.id}`, "true", { EX: 3600 });

      // Acknowledge and remove from Redis Stream
      await RedisStreamQueue.acknowledgeAndClean(streamMessage.streamName, streamMessage.streamMessageId);

      // Release lock atomically
      await lockHandle.release();

      console.log(`✅ Job completed: ${job.id} in ${(performance.now() - totalStart).toFixed(2)}ms`);
    } catch (error: any) {
      console.error("❌ Worker execution error:", error.message);

      const job = streamMessage?.data;
      if (job?.type) {
        await metrics.incrementFailed(job.type);
        try {
          await metrics.decrementProcessing(job.type);
        } catch {}
      }

      if (job?.id) {
        try {
          const currentJob = await JobStateService.getJob(job.id);
          const currentAttempts = currentJob ? currentJob.attempts : job.attempts || 0;
          const maxRetries = currentJob ? currentJob.maxRetries : job.maxRetries || 3;
          const nextAttempts = currentAttempts + 1;

          const nonRetryable = isNonRetryableError(error);

          if (!nonRetryable && nextAttempts < maxRetries) {
            console.log(`🔁 Scheduling retry for job ${job.id}, attempt ${nextAttempts}/${maxRetries}`);
            await metrics.incrementRetry(job.type);

            await JobStateService.transitionToRetry(job.id, workerId, error.message, nextAttempts);

            const delayMs = calculateBackoffDelay(nextAttempts);

            // Schedule via non-blocking delayed queue instead of blocking sleep
            await DelayedQueueManager.scheduleRetry(
              {
                id: job.id,
                userId: job.userId,
                type: job.type,
                payload: job.payload,
                priority: job.priority,
                maxRetries,
                attempts: nextAttempts,
              },
              delayMs,
            );
          } else {
            console.log(`💀 Moving job to DLQ: ${job.id} (${nonRetryable ? "Non-retryable error" : "Max retries exceeded"})`);
            await metrics.incrementDLQ(job.type);

            await JobStateService.transitionToDLQ(job.id, workerId, error.message, nextAttempts);
          }

          // Acknowledge the failed/retried stream message so it does not stay in PEL
          if (streamMessage) {
            await RedisStreamQueue.acknowledgeAndClean(streamMessage.streamName, streamMessage.streamMessageId);
          }
        } catch (innerError) {
          console.error("❌ Error in failure handling:", innerError);
        } finally {
          if (lockHandle) {
            await lockHandle.release();
          }
        }
      }
    } finally {
      endJobTimer?.();
      activeJobsCount = Math.max(0, activeJobsCount - 1);
    }
  }
}

if (require.main === module) {
  startMetricsServer(Number(process.env.WORKER_METRICS_PORT) || 3001);
  startWorker().catch((err) => {
    console.error("Fatal worker startup failure:", err);
    process.exit(1);
  });
}
