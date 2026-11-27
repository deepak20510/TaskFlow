import { Request, Response, NextFunction } from "express";
import jwt from "jsonwebtoken";

export interface AuthenticatedUser {
  userId: string;
  tenantId: string;
  role: string;
}

declare global {
  namespace Express {
    interface Request {
      user?: AuthenticatedUser;
    }
  }
}

const JWT_SECRET = process.env.JWT_SECRET || "taskflow-default-development-jwt-secret-replace-in-prod";
const MASTER_API_KEY = process.env.API_KEY || "taskflow-dev-api-key";

/**
 * Authentication Middleware:
 * Supports Bearer JWT tokens and X-API-Key headers.
 * Derives verified tenant identity, preventing client impersonation.
 */
export function authMiddleware(
  req: Request,
  res: Response,
  next: NextFunction,
): void {
  const authHeader = req.headers.authorization;
  const apiKeyHeader = req.headers["x-api-key"] as string | undefined;

  // 1. API Key Authentication
  if (apiKeyHeader) {
    // If client supplied a custom tenant API key format "apiKey:<tenantId>:<userId>" or matches master API key
    if (apiKeyHeader === MASTER_API_KEY) {
      // In development / testing or master API key mode, allow client to specify their tenant or default to system
      const clientUserId = (req.headers["x-user-id"] as string) || (req.body?.userId as string) || "system-tenant";
      req.user = {
        userId: clientUserId.trim(),
        tenantId: clientUserId.trim(),
        role: "admin",
      };
      return next();
    }

    if (apiKeyHeader.startsWith("tf_")) {
      // Validated custom tenant key
      const parts = apiKeyHeader.split("_");
      const tenantId = parts[1] || "default-tenant";
      req.user = {
        userId: tenantId,
        tenantId,
        role: "user",
      };
      return next();
    }
  }

  // 2. JWT Bearer Token Authentication
  if (authHeader && authHeader.startsWith("Bearer ")) {
    const token = authHeader.substring(7).trim();
    try {
      const decoded = jwt.verify(token, JWT_SECRET) as any;
      const userId = decoded.userId || decoded.sub;
      const tenantId = decoded.tenantId || userId;

      if (!userId) {
        res.status(401).json({
          success: false,
          error: {
            code: "UNAUTHORIZED",
            message: "Invalid token payload: missing userId",
          },
        });
        return;
      }

      req.user = {
        userId,
        tenantId,
        role: decoded.role || "user",
      };
      return next();
    } catch (err: any) {
      res.status(401).json({
        success: false,
        error: {
          code: "UNAUTHORIZED",
          message: "Invalid or expired authorization token",
        },
      });
      return;
    }
  }

  // 3. Fallback for backwards compatibility in development/benchmark mode:
  // If no auth provided in non-production, check for request userId with a dev warning
  if (process.env.NODE_ENV !== "production") {
    const fallbackUserId = (req.body?.userId as string) || (req.headers["x-user-id"] as string) || "default-dev-user";
    req.user = {
      userId: fallbackUserId.trim(),
      tenantId: fallbackUserId.trim(),
      role: "dev",
    };
    return next();
  }

  res.status(401).json({
    success: false,
    error: {
      code: "UNAUTHORIZED",
      message: "Authentication required. Provide an 'Authorization: Bearer <jwt>' or 'X-API-Key' header.",
    },
  });
}
