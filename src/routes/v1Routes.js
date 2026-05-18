import express from "express";
import {
  activateSceneKeyframeSet,
  cancelSceneJob,
  createSceneKeyframeSet,
  createSceneJob,
  deleteScene,
  deleteSceneJob,
  getJobViewer,
  getSceneJobProgress,
  getSceneJobStatus,
  listSceneKeyframeSets,
  listSceneJobs,
  runSceneJobGs
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

router.delete(
  "/scenes/:sceneId",
  sessionAuthV1Middleware,
  deleteScene
);

router.get(
  "/scenes/:sceneId/keyframe-sets",
  sessionAuthV1Middleware,
  listSceneKeyframeSets
);

router.post(
  "/scenes/:sceneId/keyframe-sets",
  sessionAuthV1Middleware,
  createSceneKeyframeSet
);

router.patch(
  "/scenes/:sceneId/keyframe-sets/:keyframeSetId/active",
  sessionAuthV1Middleware,
  activateSceneKeyframeSet
);

router.post(
  "/scenes/:sceneId/jobs",
  sessionAuthV1Middleware,
  createSceneJob
);

router.post(
  "/scenes/:sceneId/jobs/:jobId/gs",
  sessionAuthV1Middleware,
  runSceneJobGs
);

router.post(
  "/scenes/:sceneId/jobs/:jobId/cancel",
  sessionAuthV1Middleware,
  cancelSceneJob
);

router.delete(
  "/scenes/:sceneId/jobs/:jobId",
  sessionAuthV1Middleware,
  deleteSceneJob
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
