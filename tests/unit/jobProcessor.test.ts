import { describe, it, expect } from "vitest";
import { processJob } from "../../worker-service/processors/jobProcessor";
import { NonRetryableError } from "../../shared/errors/jobErrors";

describe("Job Processor & Error Classification", () => {
  it("should successfully process a valid benchmark job", async () => {
    await expect(
      processJob({
        id: "test-job-1",
        type: "benchmark",
        payload: { processingTime: 1 },
      }),
    ).resolves.not.toThrow();
  });

  it("should successfully process a valid EMAIL job", async () => {
    await expect(
      processJob({
        id: "test-email-1",
        type: "EMAIL",
        payload: { to: "recipient@test.com", processingTime: 1 },
      }),
    ).resolves.not.toThrow();
  });

  it("should throw NonRetryableError for unknown job types (preventing false completion)", async () => {
    await expect(
      processJob({
        id: "test-unknown-1",
        type: "NON_EXISTENT_TYPE",
        payload: {},
      }),
    ).rejects.toThrow(NonRetryableError);
  });

  it("should throw NonRetryableError for invalid email payload missing recipient", async () => {
    await expect(
      processJob({
        id: "test-email-bad",
        type: "EMAIL",
        payload: {},
      }),
    ).rejects.toThrow(NonRetryableError);
  });
});
