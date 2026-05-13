import express from "express";
import { sessionAuthV1Middleware } from "../middlewares/sessionAuthMiddleware.js";
import {
  completeProfileImageUpload,
  getMyProfile,
  issueProfileImageUploadPresign,
  updateMyProfile,
  uploadProfileImageToLocalStorage
} from "../controllers/userController.js";

const router = express.Router();

router.get(
  "/me/profile",
  sessionAuthV1Middleware,
  getMyProfile
);

router.patch(
  "/me/profile",
  sessionAuthV1Middleware,
  updateMyProfile
);

router.post(
  "/me/profile-image/presign",
  sessionAuthV1Middleware,
  issueProfileImageUploadPresign
);

router.put(
  "/me/profile-image/local-upload",
  sessionAuthV1Middleware,
  express.raw({
    type: ["image/jpeg", "image/png", "image/webp"],
    limit: process.env.LOCAL_PROFILE_IMAGE_UPLOAD_LIMIT ?? "5mb"
  }),
  uploadProfileImageToLocalStorage
);

router.post(
  "/me/profile-image/complete",
  sessionAuthV1Middleware,
  completeProfileImageUpload
);

export default router;
