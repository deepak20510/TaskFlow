import { Registry, collectDefaultMetrics, Gauge, Counter, Histogram } from "prom-client";
import { metrics } from "./metrics";
import { redisClient } from "../redis/redisClient";
import { getActiveWorkers } from "../cluster/workerRegistry";
import { getAllStreams } from "../utils/partition";
import { RedisStreamQueue } from "../redis/streamQueue";

export const register = new Registry();

collectDefaultMetrics({
  register,
});

/*
|--------------------------------------------------------------------------
| Cumulative Event Counters (Prometheus Counter)
|--------------------------------------------------------------------------
*/

export const processedTotal = new Counter({
  name: "queue_processed_total",
  help: "Total number of successfully processed jobs",
  labelNames: ["type"] as const,
  registers: [register],
});

export const failedTotal = new Counter({
  name: "queue_failed_total",
  help: "Total number of failed job execution attempts",
  labelNames: ["type"] as const,
  registers: [register],
});

export const retryTotal = new Counter({
  name: "queue_retry_total",
  help: "Total number of retried jobs",
  labelNames: ["type"] as const,
  registers: [register],
});

export const dlqTotal = new Counter({
  name: "queue_dlq_total",
  help: "Total number of jobs moved to the dead letter queue",
  labelNames: ["type"] as const,
  registers: [register],
});

/*
|--------------------------------------------------------------------------
| Current State Gauges (Prometheus Gauge)
|--------------------------------------------------------------------------
*/

export const processingJobs = new Gauge({
  name: "queue_processing_jobs",
  help: "Current number of jobs being processed",
  labelNames: ["type"] as const,
  registers: [register],
});

export const activeWorkers = new Gauge({
  name: "queue_active_workers",
  help: "Current number of active worker nodes",
  registers: [register],
});

export const queueDepth = new Gauge({
  name: "queue_depth",
  help: "Current number of jobs waiting in each stream/partition",
  labelNames: ["queue"] as const,
  registers: [register],
});

export const stalledJobsCount = new Gauge({
  name: "queue_stalled_jobs_total",
  help: "Number of stalled jobs identified by recovery scanner",
  registers: [register],
});

/*
|--------------------------------------------------------------------------
| Performance Histograms
|--------------------------------------------------------------------------
*/

export const jobDuration = new Histogram({
  name: "queue_job_duration_seconds",
  help: "Total time taken to process a job",
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 10],
  registers: [register],
});

export const queueWaitDuration = new Histogram({
  name: "queue_wait_duration_seconds",
  help: "Time spent waiting in queue before processing begins",
  buckets: [0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60],
  registers: [register],
});

export const dbDuration = new Histogram({
  name: "queue_db_duration_seconds",
  help: "Time spent executing database operations",
  labelNames: ["operation"] as const,
  buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5],
  registers: [register],
});

export const businessLogicDuration = new Histogram({
  name: "queue_business_logic_duration_seconds",
  help: "Time spent executing job business logic",
  buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1],
  registers: [register],
});

export const redisLatency = new Histogram({
  name: "queue_redis_duration_seconds",
  help: "Time spent executing Redis commands",
  labelNames: ["command"] as const,
  buckets: [0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1],
  registers: [register],
});

/*
|--------------------------------------------------------------------------
| Refresh Metrics from Redis State (for scrape endpoint synchronization)
|--------------------------------------------------------------------------
*/

let lastCumulativeProcessed = 0;
let lastCumulativeFailed = 0;
let lastCumulativeRetry = 0;
let lastCumulativeDlq = 0;

export async function updateMetricsFromRedis(): Promise<void> {
  try {
    const data = await metrics.getMetrics();

    // Reconcile cumulative counters monotonically
    const currentProcessed = Number(data.totals.processed || 0);
    if (currentProcessed > lastCumulativeProcessed) {
      processedTotal.labels({ type: "all" }).inc(currentProcessed - lastCumulativeProcessed);
      lastCumulativeProcessed = currentProcessed;
    }

    const currentFailed = Number(data.totals.failed || 0);
    if (currentFailed > lastCumulativeFailed) {
      failedTotal.labels({ type: "all" }).inc(currentFailed - lastCumulativeFailed);
      lastCumulativeFailed = currentFailed;
    }

    const currentRetry = Number(data.totals.retry || 0);
    if (currentRetry > lastCumulativeRetry) {
      retryTotal.labels({ type: "all" }).inc(currentRetry - lastCumulativeRetry);
      lastCumulativeRetry = currentRetry;
    }

    const currentDlq = Number(data.totals.dlq || 0);
    if (currentDlq > lastCumulativeDlq) {
      dlqTotal.labels({ type: "all" }).inc(currentDlq - lastCumulativeDlq);
      lastCumulativeDlq = currentDlq;
    }

    // Update active processing gauge
    const activeProcessing = Object.values(data.processing || {}).reduce(
      (sum, val) => sum + Number(val || 0),
      0,
    );
    processingJobs.labels({ type: "all" }).set(activeProcessing);

    // Update active workers gauge
    const workers = await getActiveWorkers();
    activeWorkers.set(workers.length);

    // Update queue depth for every stream partition
    const allStreams = getAllStreams();
    await Promise.all(
      allStreams.map(async (stream) => {
        const depth = await RedisStreamQueue.getQueueDepth(stream);
        queueDepth.labels({ queue: stream }).set(depth);
      }),
    );
  } catch (err: any) {
    console.error("[Prometheus] Error updating metrics from Redis:", err.message);
  }
}
