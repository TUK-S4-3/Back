import express from "express";
import { sessionAuthMiddleware } from "../middlewares/sessionAuthMiddleware.js";
import {
  completeVideoUpload,
  issueVideoUploadPresign
} from "../controllers/videoController.js";

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

/**
 * 영상 업로드 완료 신고
 * POST /api/videos/complete
 */
router.post(
  "/complete",
  sessionAuthMiddleware,
  completeVideoUpload
);

export default router;
