import express from "express";
import {
  createSceneJob,
  getJobViewer,
  getSceneJobProgress,
  getSceneJobStatus,
  listSceneJobs
} from "../controllers/jobController.js";
import { listMyScenes } from "../controllers/videoController.js";
import { sessionAuthV1Middleware } from "../middlewares/sessionAuthMiddleware.js";

const router = express.Router();

router.get(
  "/users/me/scenes",
  sessionAuthV1Middleware,
  listMyScenes
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
  sessionAuthV1Middleware,
  getJobViewer
);

export default router;
