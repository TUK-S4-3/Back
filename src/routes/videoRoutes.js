import express from "express";
import { sessionAuthMiddleware } from "../middlewares/sessionAuthMiddleware.js";
import { issueVideoUploadPresign } from "../controllers/videoController.js";

const router = express.Router();

/**
 * 영상 업로드용 Presigned URL 발급
 * POST /api/videos/presign
 */
router.post(
  "/presign",
  sessionAuthMiddleware,
  issueVideoUploadPresign
);

export default router;
