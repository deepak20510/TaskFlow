import { redisClient } from "../redis/redisClient";
import { RedisStreamQueue } from "../redis/streamQueue";
import { getPartitionedStream } from "../utils/partition";

const DELAYED_JOBS_KEY = "queue:delayed_jobs";

export class DelayedQueueManager {
  /**
   * Schedule a job for future retry without blocking the worker thread
   */
  static async scheduleRetry(
    job: {
      id: string;
      userId: string;
      type: string;
      payload: any;
      priority: string;
      maxRetries?: number;
      attempts: number;
    },
    delayMs: number,
  ): Promise<void> {
    const readyAt = Date.now() + Math.max(0, delayMs);
    const serialized = JSON.stringify({
      ...job,
      scheduledAt: Date.now(),
      readyAt,
    });

    await redisClient.zAdd(DELAYED_JOBS_KEY, {
      score: readyAt,
      value: serialized,
    });

    console.log(
      `⏳ [DelayedQueue] Scheduled job ${job.id} (attempt ${job.attempts}) for retry in ${delayMs}ms`,
    );
  }

  /**
   * Check and dispatch all delayed jobs whose delay has expired
   */
  static async processReadyJobs(maxJobs: number = 50): Promise<number> {
    const now = Date.now();

    // Atomic fetch & remove expired jobs from sorted set using ZPOPMIN or range+rem
    const readyItems = await redisClient.zRangeByScore(
      DELAYED_JOBS_KEY,
      0,
      now,
      {
        LIMIT: { offset: 0, count: maxJobs },
      },
    );

    if (!readyItems || readyItems.length === 0) {
      return 0;
    }

    let dispatched = 0;
    for (const item of readyItems) {
      // Atomic remove to prevent duplicate dispatch across workers
      const removed = await redisClient.zRem(DELAYED_JOBS_KEY, item);
      if (removed === 0) {
        continue;
      }

      try {
        const job = JSON.parse(item);
        const streamName = getPartitionedStream(job.priority, job.userId);

        await RedisStreamQueue.publishJob(streamName, {
          id: job.id,
          userId: job.userId,
          type: job.type,
          payload: job.payload,
          priority: job.priority,
          maxRetries: job.maxRetries,
          attempts: job.attempts,
        });

        dispatched++;
      } catch (err) {
        console.error("[DelayedQueue] Error dispatching delayed job:", err);
      }
    }

    return dispatched;
  }
}
