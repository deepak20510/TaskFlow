import { NonRetryableError } from "../../shared/errors/jobErrors";

export interface JobPayload {
  to?: string;
  file?: string;
  source?: string;
  processingTime?: number;
  [key: string]: any;
}

export async function processJob(job: {
  id: string;
  type: string;
  payload: JobPayload;
}): Promise<void> {
  if (!job.type) {
    throw new NonRetryableError("Job type is required");
  }

  const payload = job.payload || {};

  switch (job.type.toUpperCase()) {
    case "EMAIL":
      await handleEmail(payload);
      break;

    case "IMAGE":
      await handleImage(payload);
      break;

    case "BENCHMARK":
      await handleBenchmark(payload);
      break;

    default:
      // Unknown job types must throw a NonRetryableError so they are routed to DLQ rather than completed!
      throw new NonRetryableError(`Unknown job type: "${job.type}"`);
  }
}

async function handleEmail(payload: JobPayload): Promise<void> {
  if (!payload.to || typeof payload.to !== "string") {
    throw new NonRetryableError("Email job requires a valid 'to' recipient in payload");
  }

  // Simulated processing delay
  const delayMs = typeof payload.processingTime === "number" ? payload.processingTime : 50;
  await new Promise((res) => setTimeout(res, delayMs));
}

async function handleImage(payload: JobPayload): Promise<void> {
  if (!payload.file || typeof payload.file !== "string") {
    throw new NonRetryableError("Image job requires a valid 'file' string in payload");
  }

  const delayMs = typeof payload.processingTime === "number" ? payload.processingTime : 50;
  await new Promise((res) => setTimeout(res, delayMs));
}

async function handleBenchmark(payload: JobPayload): Promise<void> {
  const processingTime =
    typeof payload.processingTime === "number" && payload.processingTime >= 0
      ? payload.processingTime
      : 1;

  await new Promise((resolve) => setTimeout(resolve, processingTime));
}
