export function sendApiError(res, req, status, code, message) {
  return res.status(status).json({
    code,
    message,
    traceId: req.traceId ?? null
  });
}
