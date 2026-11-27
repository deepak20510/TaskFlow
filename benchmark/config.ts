import "dotenv/config";

const PORT = process.env.PORT || "5000";
const totalJobs = parseInt(process.env.BENCHMARK_JOBS || "100", 10);
const batchSize = Math.min(1000, Math.max(1, parseInt(process.env.BENCHMARK_BATCH_SIZE || "50", 10)));

export const benchmarkConfig = {
  TOTAL_JOBS: totalJobs,
  BATCH_SIZE: batchSize,
  API_URL: process.env.BENCHMARK_API_URL || `http://localhost:${PORT}/jobs/batch`,
  API_KEY: process.env.API_KEY || "taskflow-dev-api-key",
  BATCH_DELAY_MS: parseInt(process.env.BENCHMARK_BATCH_DELAY_MS || "0", 10),
  POLL_INTERVAL_MS: 1000,
  BENCHMARK_TIMEOUT_MS: Math.max(120000, totalJobs * 1000), // Dynamic timeout based on scale
  PRIORITY_DISTRIBUTION: {
    HIGH: 50,
    MEDIUM: 30,
    LOW: 20,
  },
  PROCESSING_TIME_MS: {
    MIN: 0,
    MAX: 10,
  },
};
