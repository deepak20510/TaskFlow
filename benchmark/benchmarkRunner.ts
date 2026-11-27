import "dotenv/config";
import { benchmarkConfig } from "./config";
import { generateLoad } from "./loadGenerator";
import { generateBenchmarkReport } from "./benchMarkReport";
import { BenchmarkResult } from "./types";
import prisma from "../api-service/dbclient";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function calculatePercentile(values: number[], percentile: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.ceil((percentile / 100) * sorted.length) - 1;
  return sorted[Math.max(0, Math.min(index, sorted.length - 1))];
}

export async function runBenchMark() {
  try {
    const startTime = performance.now();

    console.log(`🚀 Starting TaskFlow Benchmark: Target ${benchmarkConfig.TOTAL_JOBS} jobs across priorities...`);

    const loadResult = await generateLoad();
    const benchmarkUserId = loadResult.benchmarkUserId;

    console.log(
      `Load generation completed in ${(loadResult.loadEndTime - loadResult.loadStartTime).toFixed(2)} ms`,
    );

    console.log(`\nTotal jobs configured: ${loadResult.totalJobs}`);
    console.log(`Successfully submitted: ${loadResult.submittedJobs}`);
    console.log(`Submission failed: ${loadResult.submissionFailedJobs}`);

    let processingEndTime = 0;

    if (loadResult.submittedJobs > 0) {
      console.log("\nWaiting for all benchmark jobs to finish...\n");

      const MAX_WAIT_MS = benchmarkConfig.BENCHMARK_TIMEOUT_MS;
      const waitStart = Date.now();
      const waitStartPerf = performance.now();

      while (true) {
        const processedJobs = await prisma.job.count({
          where: {
            userId: benchmarkUserId,
            status: {
              in: ["COMPLETED", "DLQ"],
            },
          },
        });

        process.stdout.write(
          `\rProcessed: ${processedJobs}/${loadResult.submittedJobs}`,
        );

        if (processedJobs >= loadResult.submittedJobs) {
          break;
        }

        if (Date.now() - waitStart > MAX_WAIT_MS) {
          throw new Error(
            `Benchmark timed out after ${MAX_WAIT_MS / 1000}s while waiting for jobs to finish.`,
          );
        }

        await sleep(benchmarkConfig.POLL_INTERVAL_MS);
      }

      processingEndTime = performance.now();
      console.log(
        `\n\nQueue drain completed in ${(processingEndTime - waitStartPerf).toFixed(2)} ms`,
      );
    } else {
      console.log("\nNo jobs were successfully submitted. Skipping processing wait.");
    }

    // Query terminal jobs to calculate exact latencies
    const finishedJobs = await prisma.job.findMany({
      where: {
        userId: benchmarkUserId,
        status: { in: ["COMPLETED", "DLQ"] },
      },
      select: {
        status: true,
        createdAt: true,
        startedAt: true,
        completedAt: true,
        attempts: true,
      },
    });

    const completedJobs = finishedJobs.filter((j) => j.status === "COMPLETED").length;
    const failedJobs = finishedJobs.filter((j) => j.status === "DLQ").length;
    const retriedJobs = finishedJobs.filter((j) => j.attempts > 0).length;

    // Calculate queue wait times and processing durations
    const waitTimes: number[] = [];
    const executionDurations: number[] = [];

    for (const job of finishedJobs) {
      if (job.startedAt && job.createdAt) {
        const wait = job.startedAt.getTime() - job.createdAt.getTime();
        if (wait >= 0) waitTimes.push(wait);
      }
      if (job.completedAt && job.startedAt) {
        const duration = job.completedAt.getTime() - job.startedAt.getTime();
        if (duration >= 0) executionDurations.push(duration);
      }
    }

    const p50LatencyMs = calculatePercentile(executionDurations, 50);
    const p95LatencyMs = calculatePercentile(executionDurations, 95);
    const p99LatencyMs = calculatePercentile(executionDurations, 99);
    const avgDurationMs =
      executionDurations.length > 0
        ? executionDurations.reduce((a, b) => a + b, 0) / executionDurations.length
        : 0;
    const avgWaitTimeMs =
      waitTimes.length > 0 ? waitTimes.reduce((a, b) => a + b, 0) / waitTimes.length : 0;

    const endTime = performance.now();

    const benchmarkResult: BenchmarkResult = {
      startTime,
      endTime,
      ...loadResult,
      processingEndTime,
      completedJobs,
      failedJobs,
      retriedJobs,
      p50LatencyMs,
      p95LatencyMs,
      p99LatencyMs,
      avgDurationMs,
      avgWaitTimeMs,
    };

    generateBenchmarkReport(benchmarkResult);
    return benchmarkResult;
  } catch (error) {
    console.error("Failed to run benchmark:", error);
    throw error;
  }
}
