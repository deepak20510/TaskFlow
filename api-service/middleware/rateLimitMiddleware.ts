import { Request, Response, NextFunction } from "express";
import { redisClient } from "../../shared/redis/redisClient";
import { tokenBucketWithTtlScript } from "../../shared/redis/luaScripts";

const CAPACITY = parseInt(process.env.RATE_LIMIT_CAPACITY || "100", 10);
const REFILL_RATE = parseInt(process.env.RATE_LIMIT_REFILL_RATE || "10", 10);
const FAIL_CLOSED = process.env.RATE_LIMIT_FAIL_CLOSED === "true";

export async function rateLimitMiddleware(
  req: Request,
  res: Response,
  next: NextFunction,
) {
  try {
    // Derive rate limit key from verified authenticated identity
    const tenantId = req.user?.tenantId || req.user?.userId || "anonymous";

    const tokensKey = `rate:${tenantId}:tokens`;
    const timestampKey = `rate:${tenantId}:last_refill`;
    const now = Math.floor(Date.now() / 1000);

    const allowed = await redisClient.eval(tokenBucketWithTtlScript, {
      keys: [tokensKey, timestampKey],
      arguments: [CAPACITY.toString(), REFILL_RATE.toString(), now.toString()],
    });

    if (allowed === 0) {
      // Calculate estimated retry-after in seconds (time to refill at least 1 token)
      const retryAfterSeconds = Math.max(1, Math.ceil(1 / REFILL_RATE));
      res.setHeader("Retry-After", retryAfterSeconds.toString());

      return res.status(429).json({
        success: false,
        error: {
          code: "RATE_LIMIT_EXCEEDED",
          message: "Rate limit exceeded. Too many requests.",
          retryAfter: retryAfterSeconds,
        },
      });
    }

    next();
  } catch (error: any) {
    console.error("[RateLimiter] Error evaluating token bucket:", error.message);

    if (FAIL_CLOSED) {
      return res.status(503).json({
        success: false,
        error: {
          code: "RATE_LIMITER_UNAVAILABLE",
          message: "Rate limiting service temporarily unavailable. Failing closed.",
        },
      });
    }

    // Fail open by default so transient Redis glitches do not reject valid client traffic
    next();
  }
}
