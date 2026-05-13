import express from "express";
import { sessionAuthMiddleware } from "../middlewares/sessionAuthMiddleware.js";
import {
  completeVideoUpload,
  issueVideoUploadPresign,
  listMyScenes,
  uploadVideoToLocalStorage
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

router.put(
  "/local-upload",
  sessionAuthMiddleware,
  express.raw({
    type: "video/mp4",
    limit: process.env.LOCAL_VIDEO_UPLOAD_LIMIT ?? "5gb"
  }),
  uploadVideoToLocalStorage
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

/**
 * 로그인 사용자 본인 scene 목록 조회
 * GET /api/videos/scenes?page=1
 */
router.get(
  "/scenes",
  sessionAuthMiddleware,
  listMyScenes
);

export default router;
