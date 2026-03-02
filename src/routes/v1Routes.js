import express from "express";
import { getJobViewer, listSceneJobs } from "../controllers/jobController.js";
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

router.get(
  "/jobs/:jobId/viewer",
  sessionAuthV1Middleware,
  getJobViewer
);

export default router;
