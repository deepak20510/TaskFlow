import { describe, it, expect } from "vitest";
import {
  createJobSchema,
  createBatchJobsSchema,
} from "../../api-service/validators/job.validator";

describe("Job Input Validators (Zod)", () => {
  it("should validate and normalize valid job input", () => {
    const input = {
      type: "EMAIL",
      priority: "high",
      payload: { to: "user@example.com" },
      maxRetries: 5,
    };

    const result = createJobSchema.safeParse(input);
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.type).toBe("EMAIL");
      expect(result.data.priority).toBe("high");
      expect(result.data.maxRetries).toBe(5);
    }
  });

  it("should apply default priority and maxRetries when omitted", () => {
    const input = {
      type: "BENCHMARK",
    };

    const result = createJobSchema.safeParse(input);
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.priority).toBe("medium");
      expect(result.data.maxRetries).toBe(3);
      expect(result.data.payload).toEqual({});
    }
  });

  it("should reject empty or missing job type", () => {
    const emptyResult = createJobSchema.safeParse({ type: "  " });
    expect(emptyResult.success).toBe(false);

    const missingResult = createJobSchema.safeParse({});
    expect(missingResult.success).toBe(false);
  });

  it("should reject invalid priority values", () => {
    const invalidPriority = createJobSchema.safeParse({
      type: "EMAIL",
      priority: "urgent", // invalid
    });
    expect(invalidPriority.success).toBe(false);
  });

  it("should enforce batch size limits (1 to 1000)", () => {
    const emptyBatch = createBatchJobsSchema.safeParse({ jobs: [] });
    expect(emptyBatch.success).toBe(false);

    const validBatch = createBatchJobsSchema.safeParse({
      jobs: [
        { type: "EMAIL", payload: { to: "a@b.com" } },
        { type: "IMAGE", payload: { file: "test.png" } },
      ],
    });
    expect(validBatch.success).toBe(true);
  });
});
