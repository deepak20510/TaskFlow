import "dotenv/config";
import { connectRedis, redisClient } from "../shared/redis/redisClient";
import prisma from "../api-service/dbclient";
import { getAllStreams } from "../shared/utils/partition";
import { RedisStreamQueue } from "../shared/redis/streamQueue";
import { OutboxPublisher } from "../shared/outbox/outboxPublisher";
import { DistributedLockManager } from "../shared/redis/lockManager";

const STUCK_THRESHOLD_MS = 60000; // 60 seconds of inactivity before considering a processing job stuck
const SCANNER_INTERVAL_MS = 5000;
const SCANNER_LOCK_KEY = "recovery:scanner:lock";

async function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms));
}

export async function runRecoveryCycle(): Promise<void> {
  // Acquire distributed lock so only one scanner runs at any given moment across clusters
  const lock = await DistributedLockManager.acquireLock(SCANNER_LOCK_KEY, 15);
  if (!lock.acquired) {
    return;
  }

  try {
    // ============================================================
    // 1. OUTBOX EVENTS PUBLISHING
    // ============================================================
    const outboxResult = await OutboxPublisher.publishPendingEvents();
    if (outboxResult.processed > 0) {
      console.log(
        `📤 [Recovery] Outbox published: ${outboxResult.succeeded}/${outboxResult.processed} (failed: ${outboxResult.failed})`,
      );
    }

    // ============================================================
    // 2. RECOVER ABANDONED STREAM MESSAGES (PEL)
    // ============================================================
    const allStreams = getAllStreams();
    for (const stream of allStreams) {
      const abandonedJob = await RedisStreamQueue.claimAbandonedJob(
        stream,
        "recovery-scanner",
        undefined,
        STUCK_THRESHOLD_MS,
      );

      if (abandonedJob) {
        console.log(`♻️ [Recovery] Autoclaimed message ${abandonedJob.streamMessageId} in stream ${stream}`);
      }
    }

    // ============================================================
    // 3. RECOVER STUCK DB JOBS WHOSE WORKERS CRASHED
    // ============================================================
    const thresholdTime = new Date(Date.now() - STUCK_THRESHOLD_MS);

    const stuckJobs = await prisma.job.findMany({
      where: {
        status: "PROCESSING",
        startedAt: { lt: thresholdTime },
      },
      take: 50,
    });

    for (const job of stuckJobs) {
      if (job.attempts >= job.maxRetries) {
        // Move to DLQ if max retries reached
        await prisma.job.update({
          where: { id: job.id },
          data: {
            status: "DLQ",
            errorMessage: "Worker crashed and job exceeded max retries",
            failedAt: new Date(),
            startedAt: null,
            workerId: null,
          },
        });
        console.log(`💀 [Recovery] Job ${job.id} moved to DLQ (exceeded max retries)`);
        continue;
      }

      // Check if the worker is still actively holding a lock or heartbeat
      const lockKey = `lock:${job.id}`;
      const activeLock = await redisClient.get(lockKey);
      if (activeLock) {
        // Worker lock is still active and valid; job is just long-running
        continue;
      }

      if (job.workerId) {
        const workerAlive = await redisClient.exists(`active_worker:${job.workerId}`);
        if (workerAlive) {
          // Worker is alive and may be finishing up
          continue;
        }
      }

      console.warn(`⚠️ [Recovery] Stuck job detected (dead worker): ${job.id}`);

      // Atomically reset status to QUEUED and re-publish to Outbox
      const streamName = `${job.priority.toLowerCase()}_priority_stream:${job.id.charCodeAt(0) % 4}`;

      await prisma.$transaction(async (tx) => {
        await tx.job.update({
          where: { id: job.id },
          data: {
            status: "QUEUED",
            errorMessage: "Recovered from dead worker",
            startedAt: null,
            workerId: null,
          },
        });

        await tx.outboxEvent.create({
          data: {
            jobId: job.id,
            streamName,
            payload: {
              id: job.id,
              userId: job.userId,
              type: job.type,
              payload: job.payload,
              priority: job.priority.toLowerCase(),
              maxRetries: job.maxRetries,
              attempts: job.attempts,
            },
            status: "PENDING",
          },
        });
      });

      console.log(`♻️ [Recovery] Re-queued stuck job via Outbox: ${job.id}`);
    }
  } catch (err) {
    console.error("❌ [Recovery] Scanner error:", err);
  } finally {
    await lock.release();
  }
}

export async function startScanner(): Promise<void> {
  await connectRedis();
  console.log("🔍 Recovery scanner service started...");

  while (true) {
    try {
      await runRecoveryCycle();
    } catch (err) {
      console.error("❌ [Recovery] Loop error:", err);
    }
    await sleep(SCANNER_INTERVAL_MS);
  }
}

// Auto-start if invoked directly via ts-node
if (require.main === module) {
  startScanner().catch((err) => {
    console.error("Fatal scanner crash:", err);
    process.exit(1);
  });
}
