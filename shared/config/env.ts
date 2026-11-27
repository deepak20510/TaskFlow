import "dotenv/config";

export interface TaskFlowConfig {
  databaseUrl: string;
  redisUrl: string;
  port: number;
  workerMetricsPort: number;
  nodeEnv: string;
  apiKey: string;
  jwtSecret: string;
  rateLimit: {
    capacity: number;
    refillRate: number;
    failClosed: boolean;
  };
  backpressure: {
    softLimit: number;
    hardLimit: number;
  };
}

export function getConfig(): TaskFlowConfig {
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    console.warn("⚠️ [Config] DATABASE_URL is not set. Database operations will fail unless configured.");
  }

  return {
    databaseUrl: databaseUrl || "",
    redisUrl: process.env.REDIS_URL || "redis://localhost:6379",
    port: parseInt(process.env.PORT || "5000", 10),
    workerMetricsPort: parseInt(process.env.WORKER_METRICS_PORT || "3001", 10),
    nodeEnv: process.env.NODE_ENV || "development",
    apiKey: process.env.API_KEY || "taskflow-dev-api-key",
    jwtSecret: process.env.JWT_SECRET || "taskflow-default-development-jwt-secret-replace-in-prod",
    rateLimit: {
      capacity: parseInt(process.env.RATE_LIMIT_CAPACITY || "100", 10),
      refillRate: parseInt(process.env.RATE_LIMIT_REFILL_RATE || "10", 10),
      failClosed: process.env.RATE_LIMIT_FAIL_CLOSED === "true",
    },
    backpressure: {
      softLimit: parseInt(process.env.BACKPRESSURE_SOFT_LIMIT || "5000", 10),
      hardLimit: parseInt(process.env.BACKPRESSURE_HARD_LIMIT || "8000", 10),
    },
  };
}

export const config = getConfig();
