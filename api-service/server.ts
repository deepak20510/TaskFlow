import express, { Request, Response, NextFunction } from "express";
import dotenv from "dotenv";
import jobRouter from "./routes/job.routes";
import metricsRouter from "./routes/metrics.routes";
import { connectRedis, redisClient } from "../shared/redis/redisClient";
import { rateLimitMiddleware } from "./middleware/rateLimitMiddleware";
import { backPressureMiddleware } from "./middleware/backPressure.middleware";
import { OutboxPublisher } from "../shared/outbox/outboxPublisher";
import prisma from "./dbclient";

dotenv.config();

const app = express();
const PORT = process.env.PORT || 5000;

app.use(
  express.json({
    limit: "10mb",
  }),
);

// JSON syntax error handler
app.use((err: any, req: Request, res: Response, next: NextFunction) => {
  if (err instanceof SyntaxError && "body" in err) {
    return res.status(400).json({
      success: false,
      error: {
        code: "INVALID_JSON",
        message: "Malformed JSON in request body",
      },
    });
  }
  next(err);
});

app.get("/", (req, res) => {
  res.json({
    name: "TaskFlow API Service",
    status: "running",
    version: "1.0.0",
  });
});

app.get("/health", async (req, res) => {
  try {
    await redisClient.ping();
    await prisma.$queryRaw`SELECT 1`;
    res.status(200).json({
      status: "OK",
      redis: "UP",
      database: "UP",
      timestamp: new Date().toISOString(),
    });
  } catch (err: any) {
    res.status(503).json({
      status: "DOWN",
      error: err.message,
    });
  }
});

// Job management routes (supports both /jobs and /api/jobs)
app.use("/jobs", rateLimitMiddleware, backPressureMiddleware, jobRouter);
app.use("/api/jobs", rateLimitMiddleware, backPressureMiddleware, jobRouter);

// Prometheus & JSON metrics routes (supports both /metrics and /api/metrics)
app.use("/metrics", metricsRouter);
app.use("/api/metrics", metricsRouter);

// Global Error Handler
app.use((err: any, req: Request, res: Response, next: NextFunction) => {
  console.error("===== GLOBAL ERROR =====");
  console.error("Message:", err.message);

  const isProduction = process.env.NODE_ENV === "production";
  if (!isProduction && err.stack) {
    console.error("Stack:", err.stack);
  }

  res.status(err.status || 500).json({
    success: false,
    error: {
      code: err.code || "INTERNAL_ERROR",
      message: err.message || "An unexpected internal server error occurred",
      ...(!isProduction && err.stack ? { stack: err.stack } : {}),
    },
  });
});

let serverInstance: any = null;

async function startServer() {
  try {
    await connectRedis();

    // Start background outbox publisher daemon
    OutboxPublisher.startDaemon(2000);

    serverInstance = app.listen(PORT, () => {
      console.log(`🚀 TaskFlow API server running on port ${PORT}`);
    });
  } catch (error) {
    console.error("❌ Failed to start server:", error);
    process.exit(1);
  }
}

// Graceful Shutdown
async function handleShutdown() {
  console.log("🛑 Gracefully shutting down API server...");

  OutboxPublisher.stopDaemon();

  if (serverInstance) {
    serverInstance.close(() => {
      console.log("HTTP server closed.");
    });
  }

  try {
    await prisma.$disconnect();
    await redisClient.quit();
  } catch {}

  process.exit(0);
}

process.on("SIGINT", handleShutdown);
process.on("SIGTERM", handleShutdown);

if (require.main === module) {
  startServer();
}

export default app;
export { app, startServer };
