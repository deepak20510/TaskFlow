export interface BenchmarkResult {
  // Overall benchmark timing
  startTime: number;
  endTime: number;

  // Load generation timing
  loadStartTime: number;
  loadEndTime: number;

  // Processing window
  processingStartTime: number;
  processingEndTime: number;

  // Submission statistics
  totalJobs: number;
  totalBatches: number;
  successfulBatches: number;
  failedBatches: number;

  submittedJobs: number;
  submissionFailedJobs: number;

  // Processing statistics
  completedJobs: number;
  failedJobs: number;
  retriedJobs?: number;

  // Latencies & Percentiles (ms)
  p50LatencyMs?: number;
  p95LatencyMs?: number;
  p99LatencyMs?: number;
  avgDurationMs?: number;
  avgWaitTimeMs?: number;
}

export interface BenchmarkSummary {
  overallDuration: number;
  submissionDuration: number;
  processingDuration: number;

  successRate: number;
  failureRate: number;
  retryRate?: number;
  dlqRate?: number;

  endToEndThroughput: number;
  processingThroughput: number;

  p50LatencyMs?: number;
  p95LatencyMs?: number;
  p99LatencyMs?: number;
}
