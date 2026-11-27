import { BenchmarkResult } from "./types";

export function generateBenchmarkReport(
  benchmarkResult: BenchmarkResult,
): void {
  const {
    startTime,
    endTime,
    loadStartTime,
    loadEndTime,
    processingStartTime,
    processingEndTime,
    totalJobs,
    totalBatches,
    successfulBatches,
    failedBatches,
    submittedJobs,
    submissionFailedJobs,
    completedJobs,
    failedJobs,
    retriedJobs = 0,
    p50LatencyMs = 0,
    p95LatencyMs = 0,
    p99LatencyMs = 0,
    avgDurationMs = 0,
    avgWaitTimeMs = 0,
  } = benchmarkResult;

  // Overall benchmark duration
  const durationInSeconds = (endTime - startTime) / 1000;
  const submissionDurationInSeconds = (loadEndTime - loadStartTime) / 1000;
  const processingDurationInSeconds =
    processingStartTime > 0 && processingEndTime > 0
      ? (processingEndTime - processingStartTime) / 1000
      : 0;

  // Throughput calculations
  const endToEndThroughput =
    durationInSeconds > 0 ? submittedJobs / durationInSeconds : 0;
  const processingThroughput =
    processingDurationInSeconds > 0
      ? submittedJobs / processingDurationInSeconds
      : 0;

  // Rates
  const successRate =
    submittedJobs > 0 ? (completedJobs / submittedJobs) * 100 : 0;
  const failureRate =
    submittedJobs > 0 ? (failedJobs / submittedJobs) * 100 : 0;
  const retryRate =
    submittedJobs > 0 ? (retriedJobs / submittedJobs) * 100 : 0;

  console.log("\n==================================================");
  console.log("            TaskFlow Benchmark Report             ");
  console.log("==================================================");

  console.log("\n--- Timing ---");
  console.log(`Overall Duration          : ${durationInSeconds.toFixed(2)} sec`);
  console.log(`Submission Duration       : ${submissionDurationInSeconds.toFixed(2)} sec`);
  console.log(`Queue Drain Duration      : ${processingDurationInSeconds.toFixed(2)} sec`);

  console.log("\n--- Throughput ---");
  console.log(`End-to-End Throughput     : ${endToEndThroughput.toFixed(2)} jobs/sec`);
  console.log(`Queue-Drain Throughput    : ${processingThroughput.toFixed(2)} jobs/sec`);

  console.log("\n--- Job Statistics ---");
  console.log(`Configured Jobs           : ${totalJobs}`);
  console.log(`Submitted Jobs            : ${submittedJobs}`);
  console.log(`Submission Failed Jobs    : ${submissionFailedJobs}`);
  console.log(`Completed Jobs            : ${completedJobs}`);
  console.log(`Jobs in DLQ               : ${failedJobs}`);
  console.log(`Retried Jobs              : ${retriedJobs}`);

  console.log("\n--- Latency & Wait Times ---");
  console.log(`Average Queue Wait Time   : ${avgWaitTimeMs.toFixed(2)} ms`);
  console.log(`Average Execution Time    : ${avgDurationMs.toFixed(2)} ms`);
  console.log(`p50 Latency               : ${p50LatencyMs.toFixed(2)} ms`);
  console.log(`p95 Latency               : ${p95LatencyMs.toFixed(2)} ms`);
  console.log(`p99 Latency               : ${p99LatencyMs.toFixed(2)} ms`);

  console.log("\n--- Batch Statistics ---");
  console.log(`Total Batches             : ${totalBatches}`);
  console.log(`Successful Batches        : ${successfulBatches}`);
  console.log(`Failed Batches            : ${failedBatches}`);

  console.log("\n--- Rates ---");
  console.log(`Job Success Rate          : ${successRate.toFixed(2)}%`);
  console.log(`Job Failure Rate          : ${failureRate.toFixed(2)}%`);
  console.log(`Job Retry Rate            : ${retryRate.toFixed(2)}%`);
  console.log("==================================================\n");
}
