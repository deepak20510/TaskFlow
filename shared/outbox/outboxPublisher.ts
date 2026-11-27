import prisma from "../../api-service/dbclient";
import { RedisStreamQueue } from "../redis/streamQueue";
import { DistributedLockManager } from "../redis/lockManager";

export class OutboxPublisher {
  private static isRunning = false;
  private static daemonInterval: NodeJS.Timeout | null = null;
  private static readonly MAX_ATTEMPTS = 10;
  private static readonly BATCH_SIZE = 50;
  private static readonly LOCK_KEY = "outbox:publisher:lock";

  /**
   * Process and publish pending outbox events with atomic locking to support multiple instances
   */
  static async publishPendingEvents(): Promise<{ processed: number; succeeded: number; failed: number }> {
    // Acquire a distributed lock to prevent multiple publishers from racing on the same batch
    const lock = await DistributedLockManager.acquireLock(this.LOCK_KEY, 10);
    if (!lock.acquired) {
      return { processed: 0, succeeded: 0, failed: 0 };
    }

    let processed = 0;
    let succeeded = 0;
    let failed = 0;

    try {
      const pendingEvents = await prisma.outboxEvent.findMany({
        where: {
          status: "PENDING",
          attempts: { lt: this.MAX_ATTEMPTS },
        },
        orderBy: { createdAt: "asc" },
        take: this.BATCH_SIZE,
      });

      if (pendingEvents.length === 0) {
        return { processed: 0, succeeded: 0, failed: 0 };
      }

      processed = pendingEvents.length;

      for (const event of pendingEvents) {
        try {
          const payload = typeof event.payload === "string" ? JSON.parse(event.payload) : event.payload;

          await RedisStreamQueue.publishJob(event.streamName, {
            id: event.jobId,
            userId: payload.userId,
            type: payload.type,
            payload: payload.payload,
            priority: payload.priority,
            maxRetries: payload.maxRetries,
            attempts: payload.attempts,
            fencingToken: payload.fencingToken,
            createdAt: payload.createdAt,
          });

          await prisma.outboxEvent.update({
            where: { id: event.id },
            data: {
              status: "PUBLISHED",
              publishedAt: new Date(),
              lastError: null,
            },
          });

          succeeded++;
        } catch (err: any) {
          failed++;
          console.error(`[OutboxPublisher] Failed to publish event ${event.id} (job ${event.jobId}):`, err.message);

          const nextAttempts = event.attempts + 1;
          await prisma.outboxEvent.update({
            where: { id: event.id },
            data: {
              attempts: nextAttempts,
              lastError: err.message,
              status: nextAttempts >= this.MAX_ATTEMPTS ? "FAILED" : "PENDING",
            },
          });
        }
      }
    } finally {
      await lock.release();
    }

    return { processed, succeeded, failed };
  }

  /**
   * Attempt immediate publication of a single outbox event right after transaction commits.
   * If Redis is down, it safely remains PENDING for the background scanner to retry.
   */
  static async publishEventImmediately(eventId: string): Promise<boolean> {
    try {
      const event = await prisma.outboxEvent.findUnique({
        where: { id: eventId },
      });

      if (!event || event.status === "PUBLISHED") {
        return true;
      }

      const payload = typeof event.payload === "string" ? JSON.parse(event.payload) : event.payload;

      await RedisStreamQueue.publishJob(event.streamName, {
        id: event.jobId,
        userId: payload.userId,
        type: payload.type,
        payload: payload.payload,
        priority: payload.priority,
        maxRetries: payload.maxRetries,
        attempts: payload.attempts,
        fencingToken: payload.fencingToken,
        createdAt: payload.createdAt,
      });

      await prisma.outboxEvent.update({
        where: { id: event.id },
        data: {
          status: "PUBLISHED",
          publishedAt: new Date(),
          lastError: null,
        },
      });

      return true;
    } catch (err: any) {
      console.warn(`[OutboxPublisher] Immediate publication failed for event ${eventId}, will retry via scanner:`, err.message);
      try {
        await prisma.outboxEvent.update({
          where: { id: eventId },
          data: {
            attempts: { increment: 1 },
            lastError: err.message,
          },
        });
      } catch {}
      return false;
    }
  }

  /**
   * Start the continuous background outbox publisher loop
   */
  static startDaemon(intervalMs: number = 2000): void {
    if (this.isRunning) return;
    this.isRunning = true;

    console.log("📤 [OutboxPublisher] Daemon started...");

    this.daemonInterval = setInterval(async () => {
      try {
        await this.publishPendingEvents();
      } catch (err) {
        console.error("[OutboxPublisher] Error in daemon iteration:", err);
      }
    }, intervalMs);

    if (this.daemonInterval.unref) {
      this.daemonInterval.unref();
    }
  }

  /**
   * Stop background outbox publisher
   */
  static stopDaemon(): void {
    if (this.daemonInterval) {
      clearInterval(this.daemonInterval);
      this.daemonInterval = null;
    }
    this.isRunning = false;
    console.log("🛑 [OutboxPublisher] Daemon stopped.");
  }
}
