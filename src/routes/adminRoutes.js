import express from "express";
import { authMiddleware } from "../middlewares/authMiddleware.js";
import { adminOnly } from "../middlewares/adminOnly.js";
import {
  getAllUploads,
  uploadResultFile
} from "../controllers/adminController.js";
import { upload } from "../middlewares/uploadMiddleware.js";

const router = express.Router();

/**
 * 관리자 - 업로드 전체 목록 조회
 * GET /api/admin/uploads
 */
router.get(
  "/uploads",
  authMiddleware,
  adminOnly,
  getAllUploads
);

/**
 * 관리자 - 3D 결과 업로드
 * POST /api/admin/uploads/:id/result
 */
router.post(
  "/uploads/:id/result",
  authMiddleware,
  adminOnly,
  upload.single("file"),
  uploadResultFile
);

export default router;
