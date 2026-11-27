import "dotenv/config";
import { connectRedis, redisClient } from "../shared/redis/redisClient";

const ALERT_KEY = "alert:failure_spike";
const CHECK_INTERVAL_MS = 30000;
let isChecking = false;

export async function checkFailureSpikes(): Promise<void> {
  if (isChecking) return;
  isChecking = true;

  try {
    const now = Math.floor(Date.now() / 60000);
    // Buckets: [current minute, previous minute, 2 minutes ago]
    const buckets = [now, now - 1, now - 2];
    let totalFailed = 0;
    let totalProcessed = 0;

    const results = await Promise.all(
      buckets.map(async (b) => {
        const [failed, processed] = await Promise.all([
          redisClient.get(`metrics:failed:${b}`),
          redisClient.get(`metrics:processed:${b}`),
        ]);

        return {
          failed: Number(failed || 0),
          processed: Number(processed || 0),
        };
      }),
    );

    for (const r of results) {
      totalFailed += r.failed;
      totalProcessed += r.processed;
    }

    const totalAttempts = totalProcessed + totalFailed;
    if (totalAttempts < 10) {
      // Need minimum sample size before triggering alert
      return;
    }

    // Correct failure rate calculation: failures / total execution attempts
    const failureRate = totalFailed / totalAttempts;
    const condition = failureRate > 0.05 && totalAttempts > 20;

    const currentState = (await redisClient.get(ALERT_KEY)) ?? "ok";

    if (condition && currentState !== "firing") {
      const wasSet = await redisClient.set(ALERT_KEY, "firing", {
        NX: true,
        EX: 300,
      });

      if (wasSet === "OK") {
        console.error("🚨 [AlertService] Failure spike detected!", {
          failureRate: `${(failureRate * 100).toFixed(2)}%`,
          totalFailed,
          totalProcessed,
          totalAttempts,
        });
      }
    }

    if (!condition && currentState === "firing") {
      console.log("✅ [AlertService] Failure spike normalized. Alert resolved.");
      await redisClient.set(ALERT_KEY, "ok", { EX: 300 });
    }
  } catch (err: any) {
    console.error("[AlertService] Error during alert check:", err.message);
  } finally {
    isChecking = false;
  }
}

export async function startAlertService(): Promise<void> {
  await connectRedis();
  console.log("🔔 [AlertService] Alert monitor started...");

  setInterval(async () => {
    await checkFailureSpikes();
  }, CHECK_INTERVAL_MS);
}

if (require.main === module) {
  startAlertService().catch((err) => {
    console.error("Fatal alert service startup error:", err);
    process.exit(1);
  });
}
