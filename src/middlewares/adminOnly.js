export function adminOnly(req, res, next) {
  if (!req.user || req.user.role !== "ADMIN") {
    return res.status(403).json({
      ok: false,
      message: "관리자 권한이 필요합니다."
    });
  }
  next();
}
