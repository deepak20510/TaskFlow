import { Request, Response, NextFunction } from "express";
import { getAllStreams } from "../../shared/utils/partition";
import { RedisStreamQueue } from "../../shared/redis/streamQueue";

const HARD_LIMIT = parseInt(process.env.BACKPRESSURE_HARD_LIMIT || "8000", 10);
const SOFT_LIMIT = parseInt(process.env.BACKPRESSURE_SOFT_LIMIT || "5000", 10);
const CACHE_TTL_MS = 1000; // Cache aggregated depth for 1 second to avoid 12 Redis queries per HTTP request

let cachedTotalDepth = 0;
let lastDepthCheckTime = 0;
let isCheckingDepth = false;

async function getCachedTotalQueueDepth(): Promise<number> {
  const now = Date.now();
  if (now - lastDepthCheckTime < CACHE_TTL_MS) {
    return cachedTotalDepth;
  }

  // Prevent multiple overlapping concurrent depth scans
  if (isCheckingDepth) {
    return cachedTotalDepth;
  }

  isCheckingDepth = true;
  try {
    const allStreams = getAllStreams();
    const depths = await Promise.all(
      allStreams.map((stream) => RedisStreamQueue.getQueueDepth(stream)),
    );

    cachedTotalDepth = depths.reduce((sum, d) => sum + d, 0);
    lastDepthCheckTime = now;
  } catch (err: any) {
    console.error("[BackPressure] Error checking queue depths:", err.message);
  } finally {
    isCheckingDepth = false;
  }

  return cachedTotalDepth;
}

export async function backPressureMiddleware(
  req: Request,
  res: Response,
  next: NextFunction,
) {
  try {
    const queueSize = await getCachedTotalQueueDepth();

    // Hard reject
    if (queueSize > HARD_LIMIT) {
      console.warn("🚨 [Backpressure] Rejecting request due to system queue overload:", { queueSize, HARD_LIMIT });

      res.setHeader("Retry-After", "30");
      return res.status(429).json({
        success: false,
        error: {
          code: "SYSTEM_OVERLOADED",
          message: "System queue is currently overloaded. Please try again later.",
          retryAfter: 30,
        },
      });
    }

    // Soft slowdown
    if (queueSize > SOFT_LIMIT) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    next();
  } catch (e: any) {
    console.error("[BackPressure] Middleware error:", e.message);
    next(); // Fail open so middleware error doesn't block requests
  }
}
