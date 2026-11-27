import { describe, it, expect } from "vitest";
import { calculateBackoffDelay } from "../../shared/utils/backoff";

describe("Backoff & Jitter Calculator", () => {
  it("should calculate base exponential delay without jitter", () => {
    const delay1 = calculateBackoffDelay(1, { baseDelayMs: 1000, maxDelayMs: 10000, enableJitter: false });
    const delay2 = calculateBackoffDelay(2, { baseDelayMs: 1000, maxDelayMs: 10000, enableJitter: false });
    const delay3 = calculateBackoffDelay(3, { baseDelayMs: 1000, maxDelayMs: 10000, enableJitter: false });

    expect(delay1).toBe(1000); // 1000 * 2^0
    expect(delay2).toBe(2000); // 1000 * 2^1
    expect(delay3).toBe(4000); // 1000 * 2^2
  });

  it("should never exceed the configured maximum delay cap", () => {
    const maxDelay = 8000;
    const delay10 = calculateBackoffDelay(10, { baseDelayMs: 1000, maxDelayMs: maxDelay, enableJitter: false });
    expect(delay10).toBe(maxDelay);
  });

  it("should include non-negative randomized jitter within bounds", () => {
    const baseDelay = 1000;
    const delay = calculateBackoffDelay(1, { baseDelayMs: baseDelay, maxDelayMs: 30000, enableJitter: true });
    expect(delay).toBeGreaterThanOrEqual(baseDelay);
    expect(delay).toBeLessThanOrEqual(baseDelay + baseDelay * 0.5);
  });
});
