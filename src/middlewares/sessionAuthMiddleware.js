export function sessionAuthMiddleware(req, res, next) {
  if (!req.isAuthenticated?.() || !req.user?.id) {
    return res.status(401).json({
      ok: false,
      message: "세션 인증이 필요합니다."
    });
  }

  return next();
}
