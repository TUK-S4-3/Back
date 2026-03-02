import { sendApiError } from "../utils/apiError.js";

export function sessionAuthMiddleware(req, res, next) {
  if (!req.isAuthenticated?.() || !req.user?.id) {
    return res.status(401).json({
      ok: false,
      message: "세션 인증이 필요합니다."
    });
  }

  return next();
}

export function sessionAuthV1Middleware(req, res, next) {
  if (!req.isAuthenticated?.() || !req.user?.id) {
    return sendApiError(
      res,
      req,
      401,
      "UNAUTHORIZED",
      "세션 인증이 필요합니다."
    );
  }

  return next();
}
