import express from "express";
import { upload } from "../middlewares/uploadMiddleware.js";
import { authMiddleware } from "../middlewares/authMiddleware.js";
import {
  uploadFile,
  getMyUploads,
  downloadResultFile
} from "../controllers/uploadController.js";

const router = express.Router();

/**
 * 파일 업로드
 * POST /api/uploads
 */
router.post(
  "/",
  authMiddleware,
  upload.single("file"),
  uploadFile
);

/**
 * 내 업로드 목록 조회
 * GET /api/uploads/my
 */
router.get(
  "/my",
  authMiddleware,
  getMyUploads
);

/**
 * 결과 파일 다운로드
 * GET /api/uploads/:id/result
 */
router.get(
  "/:id/result",
  authMiddleware,
  downloadResultFile
);

export default router;
