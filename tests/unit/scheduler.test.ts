import { describe, it, expect } from "vitest";
import { Scheduler } from "../../worker-service/scheduler/schedular";

describe("Scheduler (Weighted 5:2:1 Scheduling)", () => {
  it("should return null when no queues are registered", () => {
    const scheduler = new Scheduler();
    scheduler.updateQueues([]);
    expect(scheduler.nextQueue()).toBeNull();
  });

  it("should schedule high, medium, and low queues according to 5:2:1 ratio", () => {
    const scheduler = new Scheduler();
    const queues = [
      "high_priority_queue:0",
      "medium_priority_queue:0",
      "low_priority_queue:0",
    ];
    scheduler.updateQueues(queues);

    const counts: Record<string, number> = {
      "high_priority_queue:0": 0,
      "medium_priority_queue:0": 0,
      "low_priority_queue:0": 0,
    };

    // Cycle through 8 ticks (5 + 2 + 1 = 8)
    for (let i = 0; i < 8; i++) {
      const q = scheduler.nextQueue();
      if (q) counts[q]++;
    }

    expect(counts["high_priority_queue:0"]).toBe(5);
    expect(counts["medium_priority_queue:0"]).toBe(2);
    expect(counts["low_priority_queue:0"]).toBe(1);
  });

  it("should handle empty poll backoff without starving other active queues", () => {
    const scheduler = new Scheduler();
    scheduler.updateQueues(["high_priority_queue:0", "low_priority_queue:0"]);

    // Record 3 empty polls on high priority queue to trigger backoff
    scheduler.recordResult("high_priority_queue:0", false);
    scheduler.recordResult("high_priority_queue:0", false);
    scheduler.recordResult("high_priority_queue:0", false);

    // Next queue should skip backed-off high queue and give low queue a turn
    const next = scheduler.nextQueue();
    expect(next).toBe("low_priority_queue:0");
  });
});
