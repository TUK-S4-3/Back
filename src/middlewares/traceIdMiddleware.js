import { randomUUID } from "crypto";

export function traceIdMiddleware(req, res, next) {
  const incomingTraceId = req.header("x-trace-id");
  const traceId =
    typeof incomingTraceId === "string" && incomingTraceId.trim().length > 0
      ? incomingTraceId.trim()
      : randomUUID();

  req.traceId = traceId;
  res.setHeader("x-trace-id", traceId);
  return next();
}
