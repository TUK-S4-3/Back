import express from "express";
import { sessionAuthV1Middleware } from "../middlewares/sessionAuthMiddleware.js";
import {
  completeProfileImageUpload,
  getMyProfile,
  issueProfileImageUploadPresign,
  updateMyProfile
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

router.post(
  "/me/profile-image/complete",
  sessionAuthV1Middleware,
  completeProfileImageUpload
);

export default router;
