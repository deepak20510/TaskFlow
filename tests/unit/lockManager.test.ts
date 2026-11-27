import { describe, it, expect, vi } from "vitest";
import { DistributedLockManager } from "../../shared/redis/lockManager";
import { redisClient } from "../../shared/redis/redisClient";

describe("Distributed Lock Manager", () => {
  it("should acquire lock when key does not exist", async () => {
    const setSpy = vi.spyOn(redisClient, "set").mockResolvedValueOnce("OK" as any);

    const lock = await DistributedLockManager.acquireLock("test:lock:1", 30);
    expect(lock.acquired).toBe(true);
    expect(lock.token).toBeDefined();
    expect(setSpy).toHaveBeenCalledWith("test:lock:1", lock.token, { NX: true, EX: 30 });

    setSpy.mockRestore();
  });

  it("should fail to acquire lock when key is already taken", async () => {
    const setSpy = vi.spyOn(redisClient, "set").mockResolvedValueOnce(null as any);

    const lock = await DistributedLockManager.acquireLock("test:lock:2", 30);
    expect(lock.acquired).toBe(false);

    setSpy.mockRestore();
  });

  it("should execute atomic compare-and-delete Lua script on release", async () => {
    const evalSpy = vi.spyOn(redisClient, "eval").mockResolvedValueOnce(1 as any);

    const released = await DistributedLockManager.releaseLock("test:lock:3", "token-xyz");
    expect(released).toBe(true);
    expect(evalSpy).toHaveBeenCalled();

    evalSpy.mockRestore();
  });

  it("should prevent releasing another worker's lock when token does not match", async () => {
    const evalSpy = vi.spyOn(redisClient, "eval").mockResolvedValueOnce(0 as any);

    const released = await DistributedLockManager.releaseLock("test:lock:4", "stale-token");
    expect(released).toBe(false);

    evalSpy.mockRestore();
  });
});
