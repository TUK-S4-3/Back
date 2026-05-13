import express from "express";
import {
  createSceneJob,
  getJobViewer,
  getSceneJobProgress,
  getSceneJobStatus,
  listSceneJobs
} from "../controllers/jobController.js";
import {
  completePostThumbnailUpload,
  createPost,
  deletePost,
  getPostById,
  getPostViewer,
  issuePostThumbnailUploadPresign,
  listPosts,
  uploadPostThumbnailToLocalStorage
} from "../controllers/postController.js";
import { listMyPosts } from "../controllers/userController.js";
import { listMyScenes } from "../controllers/videoController.js";
import { sessionAuthV1Middleware } from "../middlewares/sessionAuthMiddleware.js";

const router = express.Router();

router.get(
  "/users/me/scenes",
  sessionAuthV1Middleware,
  listMyScenes
);

router.get(
  "/users/me/posts",
  sessionAuthV1Middleware,
  listMyPosts
);

router.get(
  "/scenes/:sceneId/jobs",
  sessionAuthV1Middleware,
  listSceneJobs
);

router.post(
  "/scenes/:sceneId/jobs",
  sessionAuthV1Middleware,
  createSceneJob
);

router.get(
  "/scenes/:sceneId/jobs/:jobId/progress",
  sessionAuthV1Middleware,
  getSceneJobProgress
);

router.get(
  "/scenes/:sceneId/jobs/:jobId/status",
  sessionAuthV1Middleware,
  getSceneJobStatus
);

router.get(
  "/jobs/:jobId/viewer",
  getJobViewer
);

router.post(
  "/posts",
  sessionAuthV1Middleware,
  createPost
);

router.post(
  "/posts/:postId/thumbnail/presign",
  sessionAuthV1Middleware,
  issuePostThumbnailUploadPresign
);

router.put(
  "/posts/:postId/thumbnail/local-upload",
  sessionAuthV1Middleware,
  express.raw({
    type: "image/jpeg",
    limit: process.env.LOCAL_THUMBNAIL_UPLOAD_LIMIT ?? "10mb"
  }),
  uploadPostThumbnailToLocalStorage
);

router.post(
  "/posts/:postId/thumbnail/complete",
  sessionAuthV1Middleware,
  completePostThumbnailUpload
);

router.get(
  "/posts",
  listPosts
);

router.get(
  "/posts/:postId",
  getPostById
);

router.delete(
  "/posts/:postId",
  sessionAuthV1Middleware,
  deletePost
);

router.get(
  "/posts/:postId/viewer",
  getPostViewer
);

export default router;
