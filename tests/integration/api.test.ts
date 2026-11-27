import { describe, it, expect } from "vitest";
import request from "supertest";
import express from "express";
import jobRouter from "../../api-service/routes/job.routes";

const testApp = express();
testApp.use(express.json());

testApp.get("/health", (req, res) => {
  res.status(200).json({ status: "OK" });
});

testApp.use("/jobs", jobRouter);

describe("API Service Integration", () => {
  it("GET /health should return 200 OK", async () => {
    const res = await request(testApp).get("/health");
    expect(res.status).toBe(200);
    expect(res.body.status).toBe("OK");
  });

  it("POST /jobs should validate input and return 400 on empty type", async () => {
    const res = await request(testApp)
      .post("/jobs")
      .send({ type: "" });

    expect(res.status).toBe(400);
    expect(res.body.success).toBe(false);
    expect(res.body.error.code).toBe("VALIDATION_ERROR");
  });

  it("POST /jobs should reject invalid priority values", async () => {
    const res = await request(testApp)
      .post("/jobs")
      .send({ type: "EMAIL", priority: "invalid-priority" });

    expect(res.status).toBe(400);
    expect(res.body.success).toBe(false);
  });
});
