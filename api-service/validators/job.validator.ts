import { z } from "zod";

export const priorityEnum = z.enum(["high", "medium", "low"]);
export type JobPriorityInput = z.infer<typeof priorityEnum>;

export const createJobSchema = z.object({
  type: z
    .string()
    .trim()
    .min(1, "Job type cannot be empty")
    .max(50, "Job type cannot exceed 50 characters"),
  priority: priorityEnum.default("medium"),
  payload: z.record(z.string(), z.any()).default({}),
  maxRetries: z.number().int().min(0).max(10).default(3),
});

export const createBatchJobsSchema = z.object({
  jobs: z
    .array(createJobSchema)
    .min(1, "Batch must contain at least 1 job")
    .max(1000, "Batch size cannot exceed 1000 jobs"),
});

export type CreateJobInput = z.infer<typeof createJobSchema>;
export type CreateBatchJobsInput = z.infer<typeof createBatchJobsSchema>;
