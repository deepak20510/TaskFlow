export const NUM_PARTITIONS = 4;

export const PRIORITIES = ["high", "medium", "low"] as const;
export type PriorityLevel = (typeof PRIORITIES)[number];

export function hashString(str: string): number {
  let hash = 0;

  for (let i = 0; i < str.length; i++) {
    hash = (hash * 31 + str.charCodeAt(i)) >>> 0;
  }

  return hash;
}

export function getPartition(userId: string): number {
  return hashString(userId) % NUM_PARTITIONS;
}

export function getPartitionedQueue(priority: string, userId: string): string {
  const normalizedPriority = priority.toLowerCase();
  const partition = getPartition(userId);
  return `${normalizedPriority}_priority_queue:${partition}`;
}

export function getPartitionedStream(priority: string, userId: string): string {
  const normalizedPriority = priority.toLowerCase();
  const partition = getPartition(userId);
  return `${normalizedPriority}_priority_stream:${partition}`;
}

export function getAllPartitions(): string[] {
  const partitions: string[] = [];
  for (const priority of PRIORITIES) {
    for (let p = 0; p < NUM_PARTITIONS; p++) {
      partitions.push(`${priority}_priority_queue:${p}`);
    }
  }
  return partitions;
}

export function getAllStreams(): string[] {
  const streams: string[] = [];
  for (const priority of PRIORITIES) {
    for (let p = 0; p < NUM_PARTITIONS; p++) {
      streams.push(`${priority}_priority_stream:${p}`);
    }
  }
  return streams;
}
