import { describe, it, expect } from "vitest";
import {
  hashString,
  getPartition,
  getPartitionedQueue,
  getPartitionedStream,
  getAllStreams,
  getAllPartitions,
} from "../../shared/utils/partition";

describe("Partition Utility", () => {
  it("should generate deterministic hash for the same string", () => {
    const hash1 = hashString("user-12345");
    const hash2 = hashString("user-12345");
    expect(hash1).toBe(hash2);
    expect(typeof hash1).toBe("number");
  });

  it("should assign consistent partitions (0 to 3)", () => {
    const p1 = getPartition("user-alice");
    const p2 = getPartition("user-alice");
    expect(p1).toBe(p2);
    expect(p1).toBeGreaterThanOrEqual(0);
    expect(p1).toBeLessThan(4);
  });

  it("should construct valid partitioned queue names", () => {
    const queue = getPartitionedQueue("HIGH", "user-1");
    expect(queue).toMatch(/^high_priority_queue:[0-3]$/);
  });

  it("should construct valid partitioned stream names", () => {
    const stream = getPartitionedStream("MEDIUM", "user-1");
    expect(stream).toMatch(/^medium_priority_stream:[0-3]$/);
  });

  it("should return all 12 priority partitions", () => {
    const partitions = getAllPartitions();
    expect(partitions).toHaveLength(12);
    expect(partitions).toContain("high_priority_queue:0");
    expect(partitions).toContain("medium_priority_queue:1");
    expect(partitions).toContain("low_priority_queue:3");
  });

  it("should return all 12 priority streams", () => {
    const streams = getAllStreams();
    expect(streams).toHaveLength(12);
    expect(streams).toContain("high_priority_stream:0");
    expect(streams).toContain("medium_priority_stream:2");
    expect(streams).toContain("low_priority_stream:3");
  });
});
