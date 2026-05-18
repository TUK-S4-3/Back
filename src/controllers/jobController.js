import { spawn } from "node:child_process";
import { mkdir, rm } from "node:fs/promises";
import path from "node:path";
import { prisma } from "../db.config.js";
import { sendApiError } from "../utils/apiError.js";
import {
  buildJobReadModel,
  buildThumbnailSummary,
  DEFAULT_PIPELINE,
  getMetaKeys,
  VIEWER_FORMAT
} from "../utils/jobPresentation.js";
import {
  buildPublicStorageUrl,
  deleteStoragePrefix,
  getLocalStorageRoot,
  isLocalStorage,
  readJsonStorageObject,
  writeLocalStorageObject
} from "../utils/storage.js";

const DEFAULT_LIMIT = 20;
const MAX_LIMIT = 50;
const DEFAULT_AUTOMATED_IMAGE_COUNT = 0;
const DEFAULT_AUTOMATED_OVERLAP = 0;
const DEFAULT_AUTOMATED_ITERATION = 21000;
const DEFAULT_KEYFRAME_ENV = {
  KEYFRAME_EPOCHS: "200",
  KEYFRAME_PRECLUSTER_GLOBAL_PERCENTILE: "99.8",
  KEYFRAME_PRECLUSTER_LOCAL_MAD_MULTIPLIER: "6.0",
  KEYFRAME_PRECLUSTER_MIN_LOCAL_MEDIAN_RATIO: "18.0",
  KEYFRAME_PRECLUSTER_MIN_LOCAL_THRESHOLD_RATIO: "5.0",
  KEYFRAME_PRECLUSTER_MAX_NEIGHBOR_HIGH_COUNT: "0",
  SFM_MAX_OVERLAP: "100"
};
const KEYFRAME_PIPELINE = "keyframes";
const SFM_PIPELINE = "sfm";
const GS_PIPELINE = "gs";
const SUPPORTED_PIPELINES = new Set([DEFAULT_PIPELINE, KEYFRAME_PIPELINE, SFM_PIPELINE, GS_PIPELINE]);
const KS_SFM_STAGE_RUN = "KS_SFM";
const GS_STAGE_RUN = "GS";
const DEFAULT_KS_SFM_DOCKER_IMAGE = "pipeline-ks-sfm:latest";
const DEFAULT_GS_DOCKER_IMAGE = "pipeline-gs:latest";
const CANCELABLE_JOB_STATUSES = new Set(["QUEUED", "SUBMITTED", "RUNNING", "WAITING_GS"]);
const STAGE_RUN_ACTIVE_STATUSES = ["QUEUED", "SUBMITTED", "RUNNING"];
const JOB_CANCELLED_ERROR_CODE = "JOB_CANCELLED";
const JOB_CANCELLED_MESSAGE = "사용자에 의해 중단됨";

const jobBaseSelect = {
  id: true,
  sceneId: true,
  keyframeSetId: true,
  sourceJobId: true,
  status: true,
  stage: true,
  progressPercent: true,
  cancelRequested: true,
  errorCode: true,
  errorMessage: true,
  batchJobId: true,
  gsBatchJobId: true,
  updatedAt: true,
  startedAt: true,
  endedAt: true,
  createdAt: true,
  pipeline: true,
  imageCount: true,
  overlap: true,
  iteration: true,
  sfmResultKey: true,
  gaussianSplatKey: true,
  meshKey: true,
  thumbnailKey: true,
  keyframeSet: {
    select: {
      id: true,
      sceneId: true,
      version: true,
      status: true,
      storagePrefix: true,
      selectedFramesPrefix: true,
      selectedFramesCsvKey: true,
      metricsKey: true,
      configKey: true,
      frameIndexPlotKey: true,
      timelineComparisonKey: true,
      selectedFrameCount: true,
      errorMessage: true,
      createdAt: true,
      updatedAt: true
    }
  },
  post: {
    select: {
      id: true,
      status: true,
      thumbnailKey: true,
      thumbnailUpdatedAt: true,
      updatedAt: true
    }
  },
  scene: {
    select: {
      id: true,
      userId: true,
      updatedAt: true,
      gaussianSplatKey: true,
      meshKey: true,
      sfmResultKey: true,
      thumbnailKey: true,
      activeKeyframeSetId: true
    }
  }
};

const keyframeSetSelect = {
  id: true,
  sceneId: true,
  version: true,
  status: true,
  storagePrefix: true,
  selectedFramesPrefix: true,
  selectedFramesCsvKey: true,
  metricsKey: true,
  configKey: true,
  frameIndexPlotKey: true,
  timelineComparisonKey: true,
  selectedFrameCount: true,
  configHash: true,
  configJson: true,
  errorMessage: true,
  createdAt: true,
  updatedAt: true
};

function parseBigInt(value) {
  try {
    return BigInt(value);
  } catch {
    return null;
  }
}

function parseLimit(value) {
  if (value === undefined) {
    return DEFAULT_LIMIT;
  }

  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isFinite(parsed) || parsed < 1 || parsed > MAX_LIMIT) {
    return null;
  }

  return parsed;
}

function parsePositiveInt(value) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(parsed) || parsed < 1) {
    return null;
  }

  return parsed;
}

function parsePipeline(value) {
  if (value === undefined || value === null || String(value).trim().length === 0) {
    return DEFAULT_PIPELINE;
  }

  const normalized = String(value).trim().toLowerCase();
  if (!SUPPORTED_PIPELINES.has(normalized)) {
    return null;
  }

  return normalized;
}

function parseCursor(rawCursor) {
  if (rawCursor === undefined || rawCursor === null || String(rawCursor).trim().length === 0) {
    return null;
  }

  try {
    const decoded = Buffer.from(String(rawCursor), "base64").toString("utf8");
    const parsed = JSON.parse(decoded);
    const id = parseBigInt(parsed?.id);
    const createdAt = new Date(parsed?.createdAt);

    if (id === null || Number.isNaN(createdAt.getTime())) {
      return null;
    }

    return { id, createdAt };
  } catch {
    return null;
  }
}

function encodeCursor(createdAt, id) {
  return Buffer.from(
    JSON.stringify({
      createdAt: createdAt.toISOString(),
      id: id.toString()
    }),
    "utf8"
  ).toString("base64");
}

function toResponseId(value) {
  const numeric = Number(value);
  if (Number.isSafeInteger(numeric)) {
    return numeric;
  }

  return value.toString();
}

function buildKeyframeSetPrefix(sceneId, version) {
  return `scenes/${sceneId.toString()}/keyframes/v${version}`;
}

function buildKeyframeSetKeys(storagePrefix) {
  const prefix = String(storagePrefix).replace(/\/+$/, "");
  return {
    storagePrefix: prefix,
    selectedFramesPrefix: `${prefix}/selected_frames`,
    selectedFramesCsvKey: `${prefix}/selected_frames.csv`,
    metricsKey: `${prefix}/selection_metrics.json`,
    configKey: `${prefix}/config.yaml`,
    latentHtmlKey: `${prefix}/latent_space.html`,
    frameIndexPlotKey: `${prefix}/frame_index_comparison.png`,
    timelineComparisonKey: `${prefix}/timeline_comparison.mp4`,
    statusKey: `${prefix}/status.json`
  };
}

function serializeKeyframeSet(keyframeSet, activeKeyframeSetId = null) {
  if (!keyframeSet) {
    return null;
  }

  const keys = buildKeyframeSetKeys(keyframeSet.storagePrefix);
  const bucketName = process.env.S3_BUCKET_NAME;
  const active =
    activeKeyframeSetId !== null &&
    keyframeSet.id?.toString() === activeKeyframeSetId.toString();

  return {
    id: toResponseId(keyframeSet.id),
    sceneId: toResponseId(keyframeSet.sceneId),
    version: keyframeSet.version,
    status: String(keyframeSet.status ?? "").toLowerCase(),
    active,
    storagePrefix: keyframeSet.storagePrefix,
    selectedFramesPrefix: keyframeSet.selectedFramesPrefix ?? keys.selectedFramesPrefix,
    selectedFramesCsvKey: keyframeSet.selectedFramesCsvKey ?? keys.selectedFramesCsvKey,
    metricsKey: keyframeSet.metricsKey ?? keys.metricsKey,
    configKey: keyframeSet.configKey ?? keys.configKey,
    latentHtmlKey: keys.latentHtmlKey,
    latentHtmlUrl: buildPublicStorageUrl(bucketName, keys.latentHtmlKey),
    frameIndexPlotKey: keyframeSet.frameIndexPlotKey ?? keys.frameIndexPlotKey,
    frameIndexPlotUrl: buildPublicStorageUrl(bucketName, keyframeSet.frameIndexPlotKey ?? keys.frameIndexPlotKey),
    timelineComparisonKey: keyframeSet.timelineComparisonKey ?? keys.timelineComparisonKey,
    timelineComparisonUrl: buildPublicStorageUrl(bucketName, keyframeSet.timelineComparisonKey ?? keys.timelineComparisonKey),
    selectedFrameCount: keyframeSet.selectedFrameCount ?? 0,
    configHash: keyframeSet.configHash ?? null,
    configJson: keyframeSet.configJson ?? null,
    errorMessage: keyframeSet.errorMessage ?? null,
    createdAt: keyframeSet.createdAt?.toISOString?.() ?? null,
    updatedAt: keyframeSet.updatedAt?.toISOString?.() ?? null
  };
}

function pipelineToStage(pipeline) {
  if (pipeline === KEYFRAME_PIPELINE) return "keyframes";
  if (pipeline === SFM_PIPELINE) return "sfm";
  if (pipeline === GS_PIPELINE) return "gs";
  return "full";
}

function buildSfmPrefix(sceneId, jobId) {
  return `scenes/${sceneId.toString()}/sfm/${jobId.toString()}`;
}

function buildSceneStoragePrefix(sceneId) {
  return `scenes/${sceneId.toString()}`;
}

function buildJobStoragePrefixes(sceneId, jobId) {
  const sceneIdText = sceneId.toString();
  const jobIdText = jobId.toString();
  return [
    `scenes/${sceneIdText}/meta/${jobIdText}`,
    `scenes/${sceneIdText}/logs/${jobIdText}`,
    `scenes/${sceneIdText}/sfm/${jobIdText}`,
    `scenes/${sceneIdText}/gs/${jobIdText}`,
    `scenes/${sceneIdText}/thumb/${jobIdText}`
  ];
}

function isRunningJobStatus(status) {
  return status === "QUEUED" || status === "SUBMITTED" || status === "RUNNING";
}

function isCancelableJobStatus(status) {
  return CANCELABLE_JOB_STATUSES.has(status);
}

function isStoragePermissionError(err) {
  return err?.code === "EACCES" || err?.code === "EPERM";
}

async function deleteStoragePrefixes(prefixes) {
  const bucketName = process.env.S3_BUCKET_NAME;
  const results = [];
  for (const prefix of prefixes) {
    results.push(await deleteStoragePrefix(bucketName, prefix));
  }
  return results;
}

function buildCancelKey(sceneId, jobId) {
  return `scenes/${sceneId.toString()}/meta/${jobId.toString()}/cancel.json`;
}

function buildStageStatusKey(sceneId, jobId, stage) {
  return `scenes/${sceneId.toString()}/meta/${jobId.toString()}/stages/${stageRunSlug(stage)}/status.json`;
}

function buildStageOutputPrefixes(sceneId, jobId, stage) {
  const sceneIdText = sceneId.toString();
  const jobIdText = jobId.toString();
  if (stage === GS_STAGE_RUN) {
    return [`scenes/${sceneIdText}/gs/${jobIdText}`, `scenes/${sceneIdText}/thumb/${jobIdText}`];
  }
  if (stage === KS_SFM_STAGE_RUN) {
    return [`scenes/${sceneIdText}/sfm/${jobIdText}`];
  }
  return [];
}

function inferCancelableStage(job, stageRun) {
  if (stageRun?.stage === KS_SFM_STAGE_RUN || stageRun?.stage === GS_STAGE_RUN) {
    return stageRun.stage;
  }
  if (job?.stage === "GS_TRAINING" || job?.stage === "MESH_EXTRACTION") {
    return GS_STAGE_RUN;
  }
  if (job?.status === "RUNNING" || job?.status === "SUBMITTED" || job?.status === "QUEUED") {
    return KS_SFM_STAGE_RUN;
  }
  return null;
}

function buildSceneOutputClearData(scene, sceneId, jobId, stage) {
  const sceneIdText = sceneId.toString();
  const jobIdText = jobId.toString();
  const data = {};
  if (stage === KS_SFM_STAGE_RUN) {
    const sfmPrefix = `scenes/${sceneIdText}/sfm/${jobIdText}/`;
    if (scene?.sfmResultKey?.startsWith(sfmPrefix)) {
      data.sfmResultKey = null;
    }
  }
  if (stage === GS_STAGE_RUN) {
    const gsPrefix = `scenes/${sceneIdText}/gs/${jobIdText}/`;
    const thumbPrefix = `scenes/${sceneIdText}/thumb/${jobIdText}/`;
    if (scene?.gaussianSplatKey?.startsWith(gsPrefix)) {
      data.gaussianSplatKey = null;
    }
    if (scene?.meshKey?.startsWith(gsPrefix)) {
      data.meshKey = null;
    }
    if (scene?.thumbnailKey?.startsWith(thumbPrefix)) {
      data.thumbnailKey = null;
    }
  }
  return data;
}

function buildJobOutputClearData(stage) {
  if (stage === KS_SFM_STAGE_RUN) {
    return {
      sfmResultKey: null
    };
  }
  if (stage === GS_STAGE_RUN) {
    return {
      gaussianSplatKey: null,
      meshKey: null,
      thumbnailKey: null
    };
  }
  return {};
}

async function writeLocalJsonStorageObject(key, value) {
  await writeLocalStorageObject(key, `${JSON.stringify(value, null, 2)}\n`);
}

async function writeCancelMarker({ sceneId, jobId, stageRun, stage }) {
  const now = new Date().toISOString();
  await writeLocalJsonStorageObject(buildCancelKey(sceneId, jobId), {
    sceneId: toResponseId(sceneId),
    jobId: toResponseId(jobId),
    stageRunId: stageRun?.id ? toResponseId(stageRun.id) : null,
    stage: stage ?? null,
    status: "REQUESTED",
    requestedAt: now,
    updatedAt: now
  });
}

async function writeCancellationStatusDocs({ job, stageRun, stage }) {
  const now = new Date().toISOString();
  const keys = getMetaKeys(job.sceneId, job.id);
  const base = {
    sceneId: toResponseId(job.sceneId),
    jobId: toResponseId(job.id),
    stageRunId: stageRun?.id ? toResponseId(stageRun.id) : null,
    status: "CANCELLED",
    stage: job.stage ?? "CANCELLED",
    errorCode: JOB_CANCELLED_ERROR_CODE,
    errorMessage: JOB_CANCELLED_MESSAGE,
    updatedAt: now
  };
  await writeLocalJsonStorageObject(keys.progressKey, {
    ...base,
    progress: Math.min(1, Math.max(0, Number(job.progressPercent ?? 0) / 100)),
    detail: JOB_CANCELLED_MESSAGE,
    metrics: {
      frameCount: 0,
      iter: 0,
      iters: Number(job.iteration ?? 0)
    }
  });
  await writeLocalJsonStorageObject(keys.statusKey, {
    ...base,
    outputs: {}
  });
  if (stageRun && stage) {
    await writeLocalJsonStorageObject(buildStageStatusKey(job.sceneId, job.id, stage), {
      ...base,
      stage: stageRunSlug(stage),
      logsPrefix: stageRun.logsPrefix ?? null,
      outputs: stageRun.outputsJson ?? {}
    });
  }
}

function getPipelineDockerUser() {
  const configured = String(process.env.PIPELINE_DOCKER_USER ?? "").trim();
  if (configured.length > 0) {
    return configured.toLowerCase() === "none" ? null : configured;
  }

  if (typeof process.getuid !== "function" || typeof process.getgid !== "function") {
    return null;
  }

  const uid = process.getuid();
  const gid = process.getgid();
  if (!Number.isInteger(uid) || !Number.isInteger(gid)) {
    return null;
  }

  return `${uid}:${gid}`;
}

function truncateErrorMessage(error) {
  const message =
    typeof error?.message === "string" && error.message.trim().length > 0
      ? error.message.trim()
      : "처리 실패";

  return message.slice(0, 255);
}

function sanitizeContainerName(value) {
  return String(value).replace(/[^A-Za-z0-9_.-]/g, "-").slice(0, 128);
}

function appendDockerEnv(args, name, value) {
  if (value === undefined || value === null || String(value).trim().length === 0) {
    return;
  }
  args.push("-e", `${name}=${String(value)}`);
}

function runDockerCommand(args) {
  const dockerBin = process.env.PIPELINE_DOCKER_BIN ?? "docker";
  return new Promise((resolve, reject) => {
    const child = spawn(dockerBin, args, {
      stdio: ["ignore", "pipe", "pipe"]
    });
    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    child.on("error", reject);
    child.on("close", (code) => {
      resolve({
        code,
        stdout: stdout.trim(),
        stderr: stderr.trim()
      });
    });
  });
}

function isDockerMissingContainer(result) {
  const text = `${result?.stdout ?? ""}\n${result?.stderr ?? ""}`.toLowerCase();
  return text.includes("no such container") || text.includes("not found");
}

async function stopLocalDockerContainer(containerId) {
  if (!containerId) {
    return {
      stopped: false,
      missing: false,
      killed: false
    };
  }

  const timeout = Number.parseInt(String(process.env.PIPELINE_DOCKER_STOP_TIMEOUT ?? "10"), 10);
  const stopTimeout = Number.isFinite(timeout) && timeout >= 0 ? timeout : 10;
  const stopResult = await runDockerCommand(["stop", "--time", String(stopTimeout), containerId]);
  if (stopResult.code === 0) {
    return {
      stopped: true,
      missing: false,
      killed: false
    };
  }
  if (isDockerMissingContainer(stopResult)) {
    return {
      stopped: false,
      missing: true,
      killed: false
    };
  }

  const killResult = await runDockerCommand(["kill", containerId]);
  if (killResult.code === 0) {
    return {
      stopped: true,
      missing: false,
      killed: true
    };
  }
  if (isDockerMissingContainer(killResult)) {
    return {
      stopped: false,
      missing: true,
      killed: false
    };
  }

  throw new Error(killResult.stderr || stopResult.stderr || "docker container stop failed");
}

function resolvePipelineEnvValue(name) {
  const value = process.env[name];
  if (value !== undefined && value !== null && String(value).trim().length > 0) {
    return value;
  }

  return DEFAULT_KEYFRAME_ENV[name];
}

function appendOptionalPipelineEnv(args) {
  const names = [
    "KEYFRAME_GS_ITERS",
    "KEYFRAME_CANDIDATE_FPS",
    "KEYFRAME_MIN_CANDIDATE_FRAMES",
    "KEYFRAME_MAX_CANDIDATE_FRAMES",
    "KEYFRAME_FRAME_LIMIT_ENABLED",
    "KEYFRAME_TARGET_GAP_RATIO",
    "KEYFRAME_SOFT_CV_TARGET",
    "KEYFRAME_DENSITY_WEIGHT",
    "KEYFRAME_CV_WEIGHT",
    "KEYFRAME_SEARCH_STEP",
    "KEYFRAME_IMAGE_SIZE",
    "KEYFRAME_COLOR_MODE",
    "KEYFRAME_EPOCHS",
    "KEYFRAME_BATCH_SIZE",
    "KEYFRAME_LOCAL_REFINE_ITERATIONS",
    "KEYFRAME_LOCAL_REFINE_WINDOW",
    "KEYFRAME_DEVICE",
    "KEYFRAME_PRECLUSTER_ENABLED",
    "KEYFRAME_PRECLUSTER_GLOBAL_PERCENTILE",
    "KEYFRAME_PRECLUSTER_LOCAL_WINDOW",
    "KEYFRAME_PRECLUSTER_LOCAL_MAD_MULTIPLIER",
    "KEYFRAME_PRECLUSTER_MIN_LOCAL_MEDIAN_RATIO",
    "KEYFRAME_PRECLUSTER_MIN_LOCAL_THRESHOLD_RATIO",
    "KEYFRAME_PRECLUSTER_MAX_NEIGHBOR_HIGH_COUNT",
    "KEYFRAME_PRECLUSTER_MIN_CLUSTER_FRAMES",
    "KEYFRAME_PRECLUSTER_MIN_FRAMES_PER_CLUSTER",
    "SFM_MAX_OVERLAP",
    "GS_DATA_DEVICE"
  ];

  for (const name of names) {
    appendDockerEnv(args, name, resolvePipelineEnvValue(name));
  }
}

function stageRunSlug(stage) {
  return stage === GS_STAGE_RUN ? "gs" : "ks-sfm";
}

function buildStageRunMetadata(sceneId, jobId, stage, options = {}) {
  const sceneIdText = sceneId.toString();
  const jobIdText = jobId.toString();
  const slug = stageRunSlug(stage);

  return {
    configKey: `scenes/${sceneIdText}/meta/${jobIdText}/stages/${slug}/config.json`,
    logsPrefix: `scenes/${sceneIdText}/logs/${jobIdText}/${slug}`,
    outputsJson:
      stage === GS_STAGE_RUN
        ? {
            gsPrefix: `scenes/${sceneIdText}/gs/${jobIdText}`,
            gaussianSplatKey: `scenes/${sceneIdText}/gs/${jobIdText}/result.ply`
          }
        : {
            keyframesPrefix: options.keyframesPrefix ?? `scenes/${sceneIdText}/keyframes`,
            sfmPrefix: `scenes/${sceneIdText}/sfm/${jobIdText}`,
            sfmSparsePrefix: `scenes/${sceneIdText}/sfm/${jobIdText}/sparse/0`
          }
  };
}

async function createStageRunRecord({ jobId, sceneId, stage, keyframesPrefix }) {
  const metadata = buildStageRunMetadata(sceneId, jobId, stage, {
    keyframesPrefix
  });
  return prisma.job_stage_runs.create({
    data: {
      jobId,
      stage,
      status: "QUEUED",
      configKey: metadata.configKey,
      logsPrefix: metadata.logsPrefix,
      outputsJson: metadata.outputsJson
    }
  });
}

async function markStageRunSubmitted(stageRunId, containerId) {
  if (!stageRunId) return null;
  return prisma.job_stage_runs.update({
    where: {
      id: stageRunId
    },
    data: {
      containerId,
      status: "RUNNING",
      startedAt: new Date(),
      errorCode: null,
      errorMessage: null
    }
  });
}

async function markStageRunSubmitFailed(stageRunId, error) {
  if (!stageRunId) return null;
  return prisma.job_stage_runs.update({
    where: {
      id: stageRunId
    },
    data: {
      status: "FAILED",
      endedAt: new Date(),
      errorMessage: truncateErrorMessage(error)
    }
  });
}

async function findLatestActiveStageRun(jobId) {
  return prisma.job_stage_runs.findFirst({
    where: {
      jobId,
      status: {
        in: STAGE_RUN_ACTIVE_STATUSES
      }
    },
    orderBy: {
      createdAt: "desc"
    }
  });
}

async function resetLocalPipelineWorkDir(dataRoot, workName) {
  const resolvedDataRoot = path.resolve(dataRoot);
  const safeWorkName = sanitizeContainerName(workName);
  const workRoot = path.resolve(resolvedDataRoot, "work", safeWorkName);
  if (!workRoot.startsWith(`${resolvedDataRoot}${path.sep}work${path.sep}`)) {
    throw new Error(`Unsafe pipeline work directory: ${workRoot}`);
  }

  await rm(workRoot, {
    recursive: true,
    force: true
  });
  await mkdir(workRoot, {
    recursive: true
  });
}

async function submitPipelineStageToLocalDocker({
  sceneId,
  uploadId,
  jobId,
  image,
  stage,
  stageRunId,
  pipeline,
  inputVideoKey,
  imageCount,
  overlap,
  iteration,
  keyframeSetId,
  keyframesPrefix,
  sourceSfmPrefix
}) {
  if (!isLocalStorage()) {
    throw new Error("로컬 Docker 실행은 STORAGE_DRIVER=local 설정이 필요합니다.");
  }

  const dockerBin = process.env.PIPELINE_DOCKER_BIN ?? "docker";
  const storageRoot = getLocalStorageRoot();
  const dataRoot = path.dirname(storageRoot);
  const containerStorageRoot = `/data/${path.basename(storageRoot)}`;
  const containerPrefix = process.env.PIPELINE_CONTAINER_NAME_PREFIX ?? "scene-job";
  const stageSlug = stageRunSlug(stage);
  const containerName = sanitizeContainerName(
    `${containerPrefix}-${stageSlug}-${sceneId.toString()}-${jobId.toString()}`
  );
  const workName = sanitizeContainerName(`${sceneId.toString()}-${jobId.toString()}-${stageSlug}`);
  const containerWorkRoot = `/data/work/${workName}`;
  const dockerRm = String(process.env.PIPELINE_DOCKER_RM ?? "true").trim().toLowerCase() !== "false";
  const gpus = String(process.env.PIPELINE_DOCKER_GPUS ?? "all").trim();

  await resetLocalPipelineWorkDir(dataRoot, workName);

  const args = ["run", "-d"];
  if (dockerRm) {
    args.push("--rm");
  }
  if (gpus && gpus.toLowerCase() !== "none") {
    args.push("--gpus", gpus);
  }
  const dockerUser = getPipelineDockerUser();
  if (dockerUser) {
    args.push("--user", dockerUser);
  }
  args.push("--name", containerName);
  args.push("-v", `${dataRoot}:/data`);
  appendDockerEnv(args, "STORAGE_DRIVER", "local");
  appendDockerEnv(args, "LOCAL_STORAGE_ROOT", containerStorageRoot);
  appendDockerEnv(args, "WORK", containerWorkRoot);
  appendDockerEnv(args, "SCENE_ID", sceneId.toString());
  appendDockerEnv(args, "JOB_ID", jobId.toString());
  appendDockerEnv(args, "STAGE_RUN_ID", stageRunId?.toString?.() ?? stageRunId);
  appendDockerEnv(args, "UPLOAD_ID", uploadId);
  appendDockerEnv(args, "INPUT_VIDEO_KEY", inputVideoKey);
  appendDockerEnv(args, "PIPELINE", pipeline);
  appendDockerEnv(args, "PIPELINE_STAGE", stageSlug);
  appendDockerEnv(args, "IMG", imageCount);
  appendDockerEnv(args, "OVERLAP", overlap);
  appendDockerEnv(args, "ITERS", iteration);
  appendDockerEnv(args, "KEYFRAME_SET_ID", keyframeSetId?.toString?.() ?? keyframeSetId);
  appendDockerEnv(args, "KEYFRAMES_PREFIX", keyframesPrefix);
  appendDockerEnv(args, "SOURCE_SFM_PREFIX", sourceSfmPrefix);
  appendOptionalPipelineEnv(args);
  args.push(image);

  return new Promise((resolve, reject) => {
    const child = spawn(dockerBin, args, {
      stdio: ["ignore", "pipe", "pipe"]
    });
    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(stderr.trim() || `docker run failed with exit code ${code}`));
        return;
      }

      const containerId = stdout.trim();
      if (!containerId) {
        reject(new Error("Docker container id를 받지 못했습니다."));
        return;
      }

      resolve(containerId);
    });
  });
}

function submitKsSfmJobToLocalDocker(args) {
  return submitPipelineStageToLocalDocker({
    ...args,
    image: process.env.PIPELINE_KS_SFM_DOCKER_IMAGE ?? DEFAULT_KS_SFM_DOCKER_IMAGE,
    stage: KS_SFM_STAGE_RUN
  });
}

function submitGsJobToLocalDocker(args) {
  return submitPipelineStageToLocalDocker({
    ...args,
    image: process.env.PIPELINE_GS_DOCKER_IMAGE ?? DEFAULT_GS_DOCKER_IMAGE,
    stage: GS_STAGE_RUN
  });
}

async function refreshKeyframeSetFromStorage(keyframeSet) {
  if (!keyframeSet?.storagePrefix) {
    return keyframeSet;
  }

  const keys = buildKeyframeSetKeys(keyframeSet.storagePrefix);
  const [statusDoc, metricsDoc] = await Promise.all([
    readJsonStorageObject(process.env.S3_BUCKET_NAME, keys.statusKey),
    readJsonStorageObject(process.env.S3_BUCKET_NAME, keys.metricsKey)
  ]);

  const data = {};
  const rawStatus = String(statusDoc?.status ?? "").trim().toUpperCase();
  if (rawStatus === "SUCCEEDED" && keyframeSet.status !== "READY") {
    data.status = "READY";
  } else if (rawStatus === "FAILED" && keyframeSet.status !== "FAILED") {
    data.status = "FAILED";
  }

  const selectedFrameCount = Number.parseInt(
    String(
      statusDoc?.selectedFrameCount ??
        metricsDoc?.actual_selected_num_frames ??
        metricsDoc?.selected_num_frames ??
        ""
    ),
    10
  );
  if (
    Number.isFinite(selectedFrameCount) &&
    selectedFrameCount >= 0 &&
    selectedFrameCount !== keyframeSet.selectedFrameCount
  ) {
    data.selectedFrameCount = selectedFrameCount;
  }

  const errorMessage =
    typeof statusDoc?.errorMessage === "string" && statusDoc.errorMessage.trim().length > 0
      ? statusDoc.errorMessage.trim().slice(0, 255)
      : null;
  if (errorMessage && errorMessage !== keyframeSet.errorMessage) {
    data.errorMessage = errorMessage;
  }

  if (Object.keys(data).length === 0) {
    return keyframeSet;
  }

  return prisma.keyframe_sets.update({
    where: {
      id: keyframeSet.id
    },
    data,
    select: keyframeSetSelect
  });
}

async function createKeyframeSetRecord(sceneId) {
  const aggregate = await prisma.keyframe_sets.aggregate({
    where: {
      sceneId
    },
    _max: {
      version: true
    }
  });
  const version = Number(aggregate._max.version ?? 0) + 1;
  const storagePrefix = buildKeyframeSetPrefix(sceneId, version);
  const keys = buildKeyframeSetKeys(storagePrefix);

  return prisma.keyframe_sets.create({
    data: {
      sceneId,
      version,
      status: "PENDING",
      storagePrefix,
      selectedFramesPrefix: keys.selectedFramesPrefix,
      selectedFramesCsvKey: keys.selectedFramesCsvKey,
      metricsKey: keys.metricsKey,
      configKey: keys.configKey,
      frameIndexPlotKey: keys.frameIndexPlotKey,
      timelineComparisonKey: keys.timelineComparisonKey
    },
    select: keyframeSetSelect
  });
}

async function resolveJobKeyframeSet({ scene, requestedKeyframeSetId, createIfMissing }) {
  if (requestedKeyframeSetId) {
    const keyframeSet = await prisma.keyframe_sets.findFirst({
      where: {
        id: requestedKeyframeSetId,
        sceneId: scene.id
      },
      select: keyframeSetSelect
    });
    if (!keyframeSet) {
      return {
        keyframeSet: null,
        error: {
          status: 404,
          code: "KEYFRAME_SET_NOT_FOUND",
          message: "선택한 KS 버전을 찾을 수 없습니다."
        }
      };
    }

    const refreshed = await refreshKeyframeSetFromStorage(keyframeSet);
    if (refreshed.status !== "READY") {
      return {
        keyframeSet: null,
        error: {
          status: 409,
          code: "KEYFRAME_SET_NOT_READY",
          message: "READY 상태의 KS 버전만 사용할 수 있습니다."
        }
      };
    }

    return {
      keyframeSet: refreshed,
      created: false,
      error: null
    };
  }

  if (scene.activeKeyframeSetId) {
    const active = await prisma.keyframe_sets.findFirst({
      where: {
        id: scene.activeKeyframeSetId,
        sceneId: scene.id
      },
      select: keyframeSetSelect
    });
    if (active) {
      const refreshed = await refreshKeyframeSetFromStorage(active);
      if (refreshed.status === "READY") {
        return {
          keyframeSet: refreshed,
          created: false,
          error: null
        };
      }
    }
  }

  if (!createIfMissing) {
    return {
      keyframeSet: null,
      created: false,
      error: null
    };
  }

  return {
    keyframeSet: await createKeyframeSetRecord(scene.id),
    created: true,
    error: null
  };
}

async function loadOwnedScene(sceneId, userId) {
  const scene = await prisma.scenes.findUnique({
    where: {
      id: sceneId
    },
    select: {
      id: true,
      userId: true,
      title: true,
      status: true,
      uploadId: true,
      inputVideoKey: true,
      activeKeyframeSetId: true,
      gaussianSplatKey: true,
      meshKey: true,
      sfmResultKey: true,
      thumbnailKey: true
    }
  });

  if (!scene) {
    return {
      scene: null,
      error: {
        status: 404,
        code: "SCENE_NOT_FOUND",
        message: "scene을 찾을 수 없습니다."
      }
    };
  }

  if (scene.userId !== userId) {
    return {
      scene: null,
      error: {
        status: 403,
        code: "FORBIDDEN",
        message: "접근 권한이 없습니다."
      }
    };
  }

  return {
    scene,
    error: null
  };
}

async function loadOwnedJob(sceneId, jobId) {
  return prisma.jobs.findFirst({
    where: {
      id: jobId,
      sceneId
    },
    select: jobBaseSelect
  });
}

async function serializeJob(job, readModel) {
  const postId = job.post ? toResponseId(job.post.id) : null;
  const thumbnail = await buildThumbnailSummary({
    bucketName: process.env.S3_BUCKET_NAME,
    post: job.post,
    job,
    scene: job.scene
  });

  return {
    id: toResponseId(job.id),
    sceneId: toResponseId(job.sceneId),
    pipeline: job.pipeline,
    keyframeSetId: job.keyframeSetId ? toResponseId(job.keyframeSetId) : null,
    sourceJobId: job.sourceJobId ? toResponseId(job.sourceJobId) : null,
    gsBatchJobId: job.gsBatchJobId ?? null,
    keyframeSet: serializeKeyframeSet(job.keyframeSet, job.scene?.activeKeyframeSetId ?? null),
    status: readModel.status,
    stage: readModel.stage,
    progress: readModel.progress,
    cancelRequested: job.cancelRequested ?? false,
    imageCount: job.imageCount,
    overlap: job.overlap,
    iteration: job.iteration,
    createdAt: job.createdAt.toISOString(),
    updatedAt: readModel.updatedAt,
    finishedAt: readModel.finishedAt,
    errorCode: job.errorCode ?? null,
    errorMessage: readModel.status === "failed" || readModel.status === "canceled" ? readModel.detail : null,
    viewerReady: readModel.viewerReady,
    viewerKind: readModel.viewerKind,
    canRunGs: readModel.status === "waiting_gs" && Boolean(readModel.outputs.sfmResultKey),
    postable: readModel.postable,
    alreadyPosted: postId !== null,
    postId,
    resultKey: readModel.outputs.resultKey,
    resultUrl: readModel.outputs.resultUrl,
    gaussianSplatKey: readModel.outputs.gaussianSplatKey,
    gaussianSplatUrl: readModel.outputs.gaussianSplatUrl,
    meshKey: readModel.outputs.meshKey,
    meshUrl: readModel.outputs.meshUrl,
    sfmResultKey: readModel.outputs.sfmResultKey,
    sfmResultUrl: readModel.outputs.sfmResultUrl,
    thumbnailKey: thumbnail.thumbnailKey,
    thumbnailUrl: thumbnail.thumbnailUrl,
    thumbnailUpdatedAt: thumbnail.thumbnailUpdatedAt,
    outputs: readModel.outputs
  };
}

/**
 * Scene별 Job 목록 조회
 * GET /api/v1/scenes/:sceneId/jobs?cursor=&limit=20&pipeline=3dgs
 */
export async function listSceneJobs(req, res) {
  try {
    const userId = parseBigInt(req.user?.id);
    if (userId === null) {
      return sendApiError(
        res,
        req,
        401,
        "UNAUTHORIZED",
        "세션 사용자 정보가 유효하지 않습니다."
      );
    }

    const sceneId = parseBigInt(req.params?.sceneId);
    if (sceneId === null) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "sceneId는 숫자여야 합니다."
      );
    }

    const limit = parseLimit(req.query?.limit);
    if (limit === null) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "limit는 1 이상 50 이하 정수여야 합니다."
      );
    }

    const pipeline = req.query?.pipeline === undefined ? undefined : parsePipeline(req.query?.pipeline);
    if (pipeline === null) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "pipeline 값을 확인해주세요."
      );
    }

    const rawCursor = req.query?.cursor;
    const cursor = parseCursor(rawCursor);
    const hasNonEmptyCursor =
      rawCursor !== undefined &&
      rawCursor !== null &&
      String(rawCursor).trim().length > 0;

    if (hasNonEmptyCursor && cursor === null) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "cursor 형식이 올바르지 않습니다."
      );
    }

    const loadedScene = await loadOwnedScene(sceneId, userId);
    if (loadedScene.error) {
      return sendApiError(
        res,
        req,
        loadedScene.error.status,
        loadedScene.error.code,
        loadedScene.error.message
      );
    }

    const where = {
      sceneId,
      ...(pipeline ? { pipeline } : {}),
      ...(cursor
        ? {
            OR: [
              {
                createdAt: {
                  lt: cursor.createdAt
                }
              },
              {
                AND: [
                  {
                    createdAt: cursor.createdAt
                  },
                  {
                    id: {
                      lt: cursor.id
                    }
                  }
                ]
              }
            ]
          }
        : {})
    };

    const foundJobs = await prisma.jobs.findMany({
      where,
      orderBy: [
        {
          createdAt: "desc"
        },
        {
          id: "desc"
        }
      ],
      take: limit + 1,
      select: jobBaseSelect
    });

    const hasNext = foundJobs.length > limit;
    const jobs = hasNext ? foundJobs.slice(0, limit) : foundJobs;
    const readModels = await Promise.all(
      jobs.map((job) => buildJobReadModel(job, { bucketName: process.env.S3_BUCKET_NAME }))
    );

    const nextCursor =
      hasNext && jobs.length > 0
        ? encodeCursor(jobs[jobs.length - 1].createdAt, jobs[jobs.length - 1].id)
        : null;

    return res.status(200).json({
      sceneId: toResponseId(loadedScene.scene.id),
      inputVideoKey: loadedScene.scene.inputVideoKey ?? null,
      jobs: await Promise.all(
        jobs.map((job, index) => serializeJob(job, readModels[index]))
      ),
      nextCursor
    });
  } catch (err) {
    console.error(err);
    return sendApiError(
      res,
      req,
      500,
      "INTERNAL_ERROR",
      "scene job 목록 조회 실패"
    );
  }
}

export async function deleteScene(req, res) {
  try {
    const userId = parseBigInt(req.user?.id);
    if (userId === null) {
      return sendApiError(res, req, 401, "UNAUTHORIZED", "세션 사용자 정보가 유효하지 않습니다.");
    }

    const sceneId = parseBigInt(req.params?.sceneId);
    if (sceneId === null) {
      return sendApiError(res, req, 400, "BAD_REQUEST", "sceneId는 숫자여야 합니다.");
    }

    const loadedScene = await loadOwnedScene(sceneId, userId);
    if (loadedScene.error) {
      return sendApiError(
        res,
        req,
        loadedScene.error.status,
        loadedScene.error.code,
        loadedScene.error.message
      );
    }

    const runningJobCount = await prisma.jobs.count({
      where: {
        sceneId,
        status: {
          in: ["QUEUED", "SUBMITTED", "RUNNING"]
        }
      }
    });
    if (runningJobCount > 0) {
      return sendApiError(
        res,
        req,
        409,
        "SCENE_HAS_RUNNING_JOBS",
        "실행 중인 job이 있는 scene은 삭제할 수 없습니다."
      );
    }

    const storageResults = await deleteStoragePrefixes([buildSceneStoragePrefix(sceneId)]);
    await prisma.scenes.delete({
      where: {
        id: sceneId
      }
    });

    return res.status(200).json({
      ok: true,
      sceneId: toResponseId(sceneId),
      deletedStoragePrefixes: storageResults,
      message: "scene이 삭제되었습니다."
    });
  } catch (err) {
    if (err?.code === "P2025") {
      return sendApiError(res, req, 404, "SCENE_NOT_FOUND", "scene을 찾을 수 없습니다.");
    }
    if (isStoragePermissionError(err)) {
      return sendApiError(
        res,
        req,
        409,
        "STORAGE_DELETE_PERMISSION_DENIED",
        "storage 파일 권한 때문에 scene을 삭제할 수 없습니다."
      );
    }

    console.error(err);
    return sendApiError(res, req, 500, "INTERNAL_ERROR", "scene 삭제 실패");
  }
}

export async function deleteSceneJob(req, res) {
  try {
    const userId = parseBigInt(req.user?.id);
    if (userId === null) {
      return sendApiError(res, req, 401, "UNAUTHORIZED", "세션 사용자 정보가 유효하지 않습니다.");
    }

    const sceneId = parseBigInt(req.params?.sceneId);
    const jobId = parseBigInt(req.params?.jobId);
    if (sceneId === null || jobId === null) {
      return sendApiError(res, req, 400, "BAD_REQUEST", "sceneId, jobId는 숫자여야 합니다.");
    }

    const loadedScene = await loadOwnedScene(sceneId, userId);
    if (loadedScene.error) {
      return sendApiError(
        res,
        req,
        loadedScene.error.status,
        loadedScene.error.code,
        loadedScene.error.message
      );
    }

    const job = await loadOwnedJob(sceneId, jobId);
    if (!job) {
      return sendApiError(res, req, 404, "JOB_NOT_FOUND", "job을 찾을 수 없습니다.");
    }
    if (isRunningJobStatus(job.status)) {
      return sendApiError(
        res,
        req,
        409,
        "JOB_RUNNING",
        "실행 중인 job은 삭제할 수 없습니다."
      );
    }

    const storageResults = await deleteStoragePrefixes(buildJobStoragePrefixes(sceneId, jobId));
    const scene = loadedScene.scene;
    const jobSfmPrefix = buildSfmPrefix(sceneId, jobId);
    const jobGsPrefix = `scenes/${sceneId.toString()}/gs/${jobId.toString()}`;
    const jobThumbPrefix = `scenes/${sceneId.toString()}/thumb/${jobId.toString()}`;
    const sceneUpdates = {};
    if (scene.sfmResultKey?.startsWith(`${jobSfmPrefix}/`)) {
      sceneUpdates.sfmResultKey = null;
    }
    if (scene.gaussianSplatKey?.startsWith(`${jobGsPrefix}/`)) {
      sceneUpdates.gaussianSplatKey = null;
    }
    if (scene.meshKey?.startsWith(`${jobGsPrefix}/`)) {
      sceneUpdates.meshKey = null;
    }
    if (scene.thumbnailKey?.startsWith(`${jobThumbPrefix}/`)) {
      sceneUpdates.thumbnailKey = null;
    }

    const transaction = [
      prisma.jobs.delete({
        where: {
          id: jobId
        }
      })
    ];
    if (Object.keys(sceneUpdates).length > 0) {
      transaction.push(
        prisma.scenes.update({
          where: {
            id: sceneId
          },
          data: sceneUpdates
        })
      );
    }
    await prisma.$transaction(transaction);

    return res.status(200).json({
      ok: true,
      sceneId: toResponseId(sceneId),
      jobId: toResponseId(jobId),
      deletedStoragePrefixes: storageResults,
      message: "job이 삭제되었습니다."
    });
  } catch (err) {
    if (err?.code === "P2025") {
      return sendApiError(res, req, 404, "JOB_NOT_FOUND", "job을 찾을 수 없습니다.");
    }
    if (isStoragePermissionError(err)) {
      return sendApiError(
        res,
        req,
        409,
        "STORAGE_DELETE_PERMISSION_DENIED",
        "storage 파일 권한 때문에 job을 삭제할 수 없습니다."
      );
    }

    console.error(err);
    return sendApiError(res, req, 500, "INTERNAL_ERROR", "job 삭제 실패");
  }
}

export async function cancelSceneJob(req, res) {
  try {
    const userId = parseBigInt(req.user?.id);
    if (userId === null) {
      return sendApiError(res, req, 401, "UNAUTHORIZED", "세션 사용자 정보가 유효하지 않습니다.");
    }

    if (!isLocalStorage()) {
      return sendApiError(
        res,
        req,
        400,
        "LOCAL_PIPELINE_REQUIRED",
        "job 중단은 local Docker 파이프라인에서만 지원됩니다."
      );
    }

    const sceneId = parseBigInt(req.params?.sceneId);
    const jobId = parseBigInt(req.params?.jobId);
    if (sceneId === null || jobId === null) {
      return sendApiError(res, req, 400, "BAD_REQUEST", "sceneId, jobId는 숫자여야 합니다.");
    }

    const loadedScene = await loadOwnedScene(sceneId, userId);
    if (loadedScene.error) {
      return sendApiError(
        res,
        req,
        loadedScene.error.status,
        loadedScene.error.code,
        loadedScene.error.message
      );
    }

    const job = await loadOwnedJob(sceneId, jobId);
    if (!job) {
      return sendApiError(res, req, 404, "JOB_NOT_FOUND", "job을 찾을 수 없습니다.");
    }

    if (job.status === "CANCELLED") {
      return res.status(200).json({
        ok: true,
        sceneId: toResponseId(sceneId),
        jobId: toResponseId(jobId),
        status: "canceled",
        message: "이미 중단된 job입니다."
      });
    }

    if (!isCancelableJobStatus(job.status)) {
      return sendApiError(
        res,
        req,
        409,
        "JOB_NOT_CANCELABLE",
        "완료되었거나 실패한 job은 중단할 수 없습니다."
      );
    }

    const stageRun = await findLatestActiveStageRun(job.id);
    const stage = inferCancelableStage(job, stageRun);
    await prisma.jobs.update({
      where: {
        id: job.id
      },
      data: {
        cancelRequested: true,
        errorCode: JOB_CANCELLED_ERROR_CODE,
        errorMessage: JOB_CANCELLED_MESSAGE
      }
    });
    await writeCancelMarker({
      sceneId,
      jobId,
      stageRun,
      stage
    });

    const dockerResult = stageRun?.containerId
      ? await stopLocalDockerContainer(stageRun.containerId)
      : {
          stopped: false,
          missing: false,
          killed: false
        };

    const outputPrefixes = stage ? buildStageOutputPrefixes(sceneId, jobId, stage) : [];
    const deletedStoragePrefixes = await deleteStoragePrefixes(outputPrefixes);
    const endedAt = new Date();
    const jobOutputData = buildJobOutputClearData(stage);
    const sceneOutputData = buildSceneOutputClearData(loadedScene.scene, sceneId, jobId, stage);
    const updates = [
      prisma.jobs.update({
        where: {
          id: job.id
        },
        data: {
          ...jobOutputData,
          status: "CANCELLED",
          cancelRequested: true,
          endedAt,
          errorCode: JOB_CANCELLED_ERROR_CODE,
          errorMessage: JOB_CANCELLED_MESSAGE
        }
      })
    ];

    if (stageRun) {
      updates.push(
        prisma.job_stage_runs.update({
          where: {
            id: stageRun.id
          },
          data: {
            status: "CANCELLED",
            endedAt,
            errorCode: JOB_CANCELLED_ERROR_CODE,
            errorMessage: JOB_CANCELLED_MESSAGE
          }
        })
      );
    }

    if (Object.keys(sceneOutputData).length > 0) {
      updates.push(
        prisma.scenes.update({
          where: {
            id: sceneId
          },
          data: sceneOutputData
        })
      );
    }

    await prisma.$transaction(updates);
    await writeCancellationStatusDocs({
      job: {
        ...job,
        status: "CANCELLED",
        cancelRequested: true,
        endedAt
      },
      stageRun,
      stage
    });

    return res.status(200).json({
      ok: true,
      sceneId: toResponseId(sceneId),
      jobId: toResponseId(jobId),
      status: "canceled",
      stage: stage ?? null,
      docker: dockerResult,
      deletedStoragePrefixes,
      message: "job 중단을 요청했습니다."
    });
  } catch (err) {
    if (isStoragePermissionError(err)) {
      return sendApiError(
        res,
        req,
        409,
        "STORAGE_DELETE_PERMISSION_DENIED",
        "storage 파일 권한 때문에 job 중단 정리를 완료할 수 없습니다."
      );
    }
    console.error(err);
    return sendApiError(res, req, 500, "INTERNAL_ERROR", "job 중단 실패");
  }
}

export async function listSceneKeyframeSets(req, res) {
  try {
    const userId = parseBigInt(req.user?.id);
    if (userId === null) {
      return sendApiError(res, req, 401, "UNAUTHORIZED", "세션 사용자 정보가 유효하지 않습니다.");
    }

    const sceneId = parseBigInt(req.params?.sceneId);
    if (sceneId === null) {
      return sendApiError(res, req, 400, "BAD_REQUEST", "sceneId는 숫자여야 합니다.");
    }

    const loadedScene = await loadOwnedScene(sceneId, userId);
    if (loadedScene.error) {
      return sendApiError(
        res,
        req,
        loadedScene.error.status,
        loadedScene.error.code,
        loadedScene.error.message
      );
    }

    const keyframeSets = await prisma.keyframe_sets.findMany({
      where: {
        sceneId
      },
      orderBy: {
        version: "desc"
      },
      select: keyframeSetSelect
    });
    const refreshed = await Promise.all(
      keyframeSets.map((keyframeSet) => refreshKeyframeSetFromStorage(keyframeSet))
    );
    let activeKeyframeSetId = loadedScene.scene.activeKeyframeSetId;
    if (!activeKeyframeSetId) {
      const latestReady = refreshed.find((keyframeSet) => keyframeSet.status === "READY");
      if (latestReady) {
        await prisma.scenes.update({
          where: {
            id: sceneId
          },
          data: {
            activeKeyframeSetId: latestReady.id
          }
        });
        activeKeyframeSetId = latestReady.id;
      }
    }

    return res.status(200).json({
      sceneId: toResponseId(sceneId),
      activeKeyframeSetId: activeKeyframeSetId
        ? toResponseId(activeKeyframeSetId)
        : null,
      keyframeSets: refreshed.map((keyframeSet) =>
        serializeKeyframeSet(keyframeSet, activeKeyframeSetId)
      )
    });
  } catch (err) {
    console.error(err);
    return sendApiError(res, req, 500, "INTERNAL_ERROR", "KS 버전 목록 조회 실패");
  }
}

export async function createSceneKeyframeSet(req, res) {
  try {
    const userId = parseBigInt(req.user?.id);
    if (userId === null) {
      return sendApiError(res, req, 401, "UNAUTHORIZED", "세션 사용자 정보가 유효하지 않습니다.");
    }

    const sceneId = parseBigInt(req.params?.sceneId);
    if (sceneId === null) {
      return sendApiError(res, req, 400, "BAD_REQUEST", "sceneId는 숫자여야 합니다.");
    }

    const loadedScene = await loadOwnedScene(sceneId, userId);
    if (loadedScene.error) {
      return sendApiError(
        res,
        req,
        loadedScene.error.status,
        loadedScene.error.code,
        loadedScene.error.message
      );
    }

    const scene = loadedScene.scene;
    if (!scene.uploadId || !scene.inputVideoKey) {
      return sendApiError(res, req, 400, "BAD_REQUEST", "업로드 완료된 scene만 KS+SfM job 생성이 가능합니다.");
    }

    const iteration = parsePositiveInt(process.env.KEYFRAME_GS_ITERS) ?? DEFAULT_AUTOMATED_ITERATION;
    const keyframeSet = await createKeyframeSetRecord(sceneId);
    const job = await prisma.jobs.create({
      data: {
        sceneId,
        keyframeSetId: keyframeSet.id,
        uploadId: scene.uploadId,
        pipeline: DEFAULT_PIPELINE,
        imageCount: DEFAULT_AUTOMATED_IMAGE_COUNT,
        overlap: DEFAULT_AUTOMATED_OVERLAP,
        iteration,
        status: "QUEUED",
        stage: "INSTANCE_CREATING",
        progressPercent: 0
      }
    });

    let submittedJobId = null;
    const runner = "local";
    const keys = buildKeyframeSetKeys(keyframeSet.storagePrefix);
    let stageRun = null;
    try {
      stageRun = await createStageRunRecord({
        jobId: job.id,
        sceneId,
        stage: KS_SFM_STAGE_RUN,
        keyframesPrefix: keyframeSet.storagePrefix
      });
      submittedJobId = await submitKsSfmJobToLocalDocker({
        sceneId,
        uploadId: scene.uploadId,
        jobId: job.id,
        stageRunId: stageRun.id,
        imageCount: DEFAULT_AUTOMATED_IMAGE_COUNT,
        overlap: DEFAULT_AUTOMATED_OVERLAP,
        iteration,
        pipeline: DEFAULT_PIPELINE,
        inputVideoKey: scene.inputVideoKey,
        keyframeSetId: keyframeSet.id,
        keyframesPrefix: keyframeSet.storagePrefix
      });
      await markStageRunSubmitted(stageRun.id, submittedJobId);
    } catch (submitErr) {
      await markStageRunSubmitFailed(stageRun?.id, submitErr);
      await prisma.$transaction([
        prisma.jobs.update({
          where: {
            id: job.id
          },
          data: {
            status: "FAILED",
            errorMessage: truncateErrorMessage(submitErr),
            endedAt: new Date()
          }
        }),
        prisma.keyframe_sets.update({
          where: {
            id: keyframeSet.id
          },
          data: {
            status: "FAILED",
            errorMessage: truncateErrorMessage(submitErr)
          }
        })
      ]);
      console.error(submitErr);
      return sendApiError(res, req, 500, "INTERNAL_ERROR", "KS+SfM 작업 제출 실패");
    }

    const [updatedJob, updatedKeyframeSet] = await prisma.$transaction([
      prisma.jobs.update({
        where: {
          id: job.id
        },
        data: {
          batchJobId: submittedJobId,
          status: "SUBMITTED"
        }
      }),
      prisma.keyframe_sets.update({
        where: {
          id: keyframeSet.id
        },
        data: {
          status: "RUNNING"
        },
        select: keyframeSetSelect
      })
    ]);

    return res.status(202).json({
      keyframeSet: serializeKeyframeSet(updatedKeyframeSet, scene.activeKeyframeSetId),
      jobId: toResponseId(updatedJob.id),
      batchJobId: submittedJobId,
      runner,
      pipeline: DEFAULT_PIPELINE,
      iteration,
      statusKey: keys.statusKey
    });
  } catch (err) {
    console.error(err);
    return sendApiError(res, req, 500, "INTERNAL_ERROR", "KS+SfM job 생성 실패");
  }
}

export async function activateSceneKeyframeSet(req, res) {
  try {
    const userId = parseBigInt(req.user?.id);
    if (userId === null) {
      return sendApiError(res, req, 401, "UNAUTHORIZED", "세션 사용자 정보가 유효하지 않습니다.");
    }

    const sceneId = parseBigInt(req.params?.sceneId);
    const keyframeSetId = parseBigInt(req.params?.keyframeSetId);
    if (sceneId === null || keyframeSetId === null) {
      return sendApiError(res, req, 400, "BAD_REQUEST", "sceneId와 keyframeSetId는 숫자여야 합니다.");
    }

    const loadedScene = await loadOwnedScene(sceneId, userId);
    if (loadedScene.error) {
      return sendApiError(
        res,
        req,
        loadedScene.error.status,
        loadedScene.error.code,
        loadedScene.error.message
      );
    }

    const keyframeSet = await prisma.keyframe_sets.findFirst({
      where: {
        id: keyframeSetId,
        sceneId
      },
      select: keyframeSetSelect
    });
    if (!keyframeSet) {
      return sendApiError(res, req, 404, "KEYFRAME_SET_NOT_FOUND", "KS 버전을 찾을 수 없습니다.");
    }

    const refreshed = await refreshKeyframeSetFromStorage(keyframeSet);
    if (refreshed.status !== "READY") {
      return sendApiError(res, req, 409, "KEYFRAME_SET_NOT_READY", "READY 상태의 KS 버전만 active로 지정할 수 있습니다.");
    }

    await prisma.scenes.update({
      where: {
        id: sceneId
      },
      data: {
        activeKeyframeSetId: keyframeSetId
      }
    });

    return res.status(200).json({
      sceneId: toResponseId(sceneId),
      activeKeyframeSetId: toResponseId(keyframeSetId),
      keyframeSet: serializeKeyframeSet(refreshed, keyframeSetId)
    });
  } catch (err) {
    console.error(err);
    return sendApiError(res, req, 500, "INTERNAL_ERROR", "active KS 버전 변경 실패");
  }
}

/**
 * Scene별 Job 생성 + 파이프라인 제출
 * POST /api/v1/scenes/:sceneId/jobs
 */
export async function createSceneJob(req, res) {
  try {
    const userId = parseBigInt(req.user?.id);
    if (userId === null) {
      return sendApiError(
        res,
        req,
        401,
        "UNAUTHORIZED",
        "세션 사용자 정보가 유효하지 않습니다."
      );
    }

    const sceneId = parseBigInt(req.params?.sceneId);
    if (sceneId === null) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "sceneId는 숫자여야 합니다."
      );
    }

    const requestedPipeline = parsePipeline(req.body?.pipeline);
    const requestedKeyframeSetIdRaw = req.body?.keyframeSetId;
    const requestedKeyframeSetId =
      requestedKeyframeSetIdRaw === undefined ||
      requestedKeyframeSetIdRaw === null ||
      String(requestedKeyframeSetIdRaw).trim().length === 0
        ? null
        : parseBigInt(requestedKeyframeSetIdRaw);
    const imageCount = DEFAULT_AUTOMATED_IMAGE_COUNT;
    const overlap = DEFAULT_AUTOMATED_OVERLAP;
    const iteration = parsePositiveInt(process.env.KEYFRAME_GS_ITERS) ?? DEFAULT_AUTOMATED_ITERATION;

    if (requestedPipeline === null) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "pipeline(3dgs) 값을 확인해주세요."
      );
    }
    if (requestedPipeline === KEYFRAME_PIPELINE || requestedPipeline === GS_PIPELINE) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "Job 생성은 KS+SfM 단계만 지원합니다. GS는 기존 job의 GS 실행 API를 사용해주세요."
      );
    }
    if (
      requestedKeyframeSetIdRaw !== undefined &&
      requestedKeyframeSetIdRaw !== null &&
      String(requestedKeyframeSetIdRaw).trim().length > 0 &&
      requestedKeyframeSetId === null
    ) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "keyframeSetId는 숫자여야 합니다."
      );
    }

    const loadedScene = await loadOwnedScene(sceneId, userId);
    if (loadedScene.error) {
      return sendApiError(
        res,
        req,
        loadedScene.error.status,
        loadedScene.error.code,
        loadedScene.error.message
      );
    }

    const scene = loadedScene.scene;
    if (!scene.uploadId || !scene.inputVideoKey) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "업로드 완료된 scene만 job 생성이 가능합니다."
      );
    }

    const resolvedKeyframeSet = await resolveJobKeyframeSet({
      scene,
      requestedKeyframeSetId,
      createIfMissing: true
    });
    if (resolvedKeyframeSet.error) {
      return sendApiError(
        res,
        req,
        resolvedKeyframeSet.error.status,
        resolvedKeyframeSet.error.code,
        resolvedKeyframeSet.error.message
      );
    }
    if (!resolvedKeyframeSet.keyframeSet) {
      return sendApiError(
        res,
        req,
        409,
        "KEYFRAME_SET_REQUIRED",
        "사용 가능한 KS 버전이 없습니다."
      );
    }

    const jobKeyframeSet = resolvedKeyframeSet.keyframeSet;

    const created = await prisma.jobs.create({
      data: {
        sceneId,
        keyframeSetId: jobKeyframeSet?.id ?? null,
        sourceJobId: null,
        uploadId: scene.uploadId,
        pipeline: DEFAULT_PIPELINE,
        imageCount,
        overlap,
        iteration,
        status: "QUEUED",
        stage: "INSTANCE_CREATING",
        progressPercent: 0
      }
    });

    let submittedJobId = null;
    const runner = "local";
    let stageRun = null;
    try {
      stageRun = await createStageRunRecord({
        jobId: created.id,
        sceneId,
        stage: KS_SFM_STAGE_RUN,
        keyframesPrefix: jobKeyframeSet?.storagePrefix ?? null
      });
      submittedJobId = await submitKsSfmJobToLocalDocker({
        sceneId,
        uploadId: scene.uploadId,
        jobId: created.id,
        stageRunId: stageRun.id,
        imageCount,
        overlap,
        iteration,
        pipeline: DEFAULT_PIPELINE,
        inputVideoKey: scene.inputVideoKey,
        keyframeSetId: jobKeyframeSet?.id ?? null,
        keyframesPrefix: jobKeyframeSet?.storagePrefix ?? null
      });
      await markStageRunSubmitted(stageRun.id, submittedJobId);
    } catch (submitErr) {
      await markStageRunSubmitFailed(stageRun?.id, submitErr);
      const updates = [
        prisma.jobs.update({
          where: {
            id: created.id
          },
          data: {
            status: "FAILED",
            errorMessage: truncateErrorMessage(submitErr),
            endedAt: new Date()
          }
        })
      ];
      if (resolvedKeyframeSet.created && resolvedKeyframeSet.keyframeSet) {
        updates.push(
          prisma.keyframe_sets.update({
            where: {
              id: resolvedKeyframeSet.keyframeSet.id
            },
            data: {
              status: "FAILED",
              errorMessage: truncateErrorMessage(submitErr)
            }
          })
        );
      }
      await prisma.$transaction(updates);

      console.error(submitErr);
      return sendApiError(
        res,
        req,
        500,
        "INTERNAL_ERROR",
        "파이프라인 작업 제출 실패"
      );
    }

    const updated = await prisma.jobs.update({
      where: {
        id: created.id
      },
      data: {
        batchJobId: submittedJobId,
        status: "SUBMITTED"
      },
      select: jobBaseSelect
    });
    if (resolvedKeyframeSet.created && resolvedKeyframeSet.keyframeSet) {
      await prisma.keyframe_sets.update({
        where: {
          id: resolvedKeyframeSet.keyframeSet.id
        },
        data: {
          status: "RUNNING"
        }
      });
    }

    const keys = getMetaKeys(sceneId, updated.id);

    return res.status(201).json({
      jobId: toResponseId(updated.id),
      sceneId: toResponseId(scene.id),
      pipeline: DEFAULT_PIPELINE,
      imageCount,
      overlap,
      iteration,
      status: "queued",
      batchJobId: submittedJobId,
      runner,
      keyframeSet: serializeKeyframeSet(updated.keyframeSet, scene.activeKeyframeSetId),
      progressKey: keys.progressKey,
      statusKey: keys.statusKey
    });
  } catch (err) {
    console.error(err);
    return sendApiError(
      res,
      req,
      500,
      "INTERNAL_ERROR",
      "scene job 생성 실패"
    );
  }
}

export async function runSceneJobGs(req, res) {
  try {
    const userId = parseBigInt(req.user?.id);
    if (userId === null) {
      return sendApiError(
        res,
        req,
        401,
        "UNAUTHORIZED",
        "세션 사용자 정보가 유효하지 않습니다."
      );
    }

    const sceneId = parseBigInt(req.params?.sceneId);
    const jobId = parseBigInt(req.params?.jobId);
    if (sceneId === null || jobId === null) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "sceneId, jobId는 숫자여야 합니다."
      );
    }

    const loadedScene = await loadOwnedScene(sceneId, userId);
    if (loadedScene.error) {
      return sendApiError(
        res,
        req,
        loadedScene.error.status,
        loadedScene.error.code,
        loadedScene.error.message
      );
    }

    const scene = loadedScene.scene;
    if (!scene.uploadId || !scene.inputVideoKey) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "업로드 완료된 scene만 GS 실행이 가능합니다."
      );
    }

    const job = await loadOwnedJob(sceneId, jobId);
    if (!job) {
      return sendApiError(
        res,
        req,
        404,
        "JOB_NOT_FOUND",
        "job을 찾을 수 없습니다."
      );
    }

    const readModel = await buildJobReadModel(job, {
      bucketName: process.env.S3_BUCKET_NAME,
      viewerKind: req.query?.view ?? req.query?.viewerKind ?? req.query?.kind
    });
    if (readModel.status === "queued" || readModel.status === "processing") {
      return sendApiError(
        res,
        req,
        409,
        "JOB_ALREADY_RUNNING",
        "현재 실행 중인 job입니다."
      );
    }
    if (!readModel.outputs.sfmResultKey) {
      return sendApiError(
        res,
        req,
        409,
        "SFM_RESULT_REQUIRED",
        "GS 실행에는 먼저 완료된 SfM 결과가 필요합니다."
      );
    }
    if (readModel.status === "ready" && readModel.outputs.gaussianSplatKey) {
      return sendApiError(
        res,
        req,
        409,
        "GS_ALREADY_READY",
        "이미 GS 결과가 준비된 job입니다."
      );
    }

    const sourceSfmPrefix = buildSfmPrefix(sceneId, job.id);
    let submittedJobId = null;
    const runner = "local";
    let stageRun = null;
    try {
      stageRun = await createStageRunRecord({
        jobId: job.id,
        sceneId,
        stage: GS_STAGE_RUN,
        keyframesPrefix: job.keyframeSet?.storagePrefix ?? null
      });
      submittedJobId = await submitGsJobToLocalDocker({
        sceneId,
        uploadId: scene.uploadId,
        jobId: job.id,
        stageRunId: stageRun.id,
        imageCount: job.imageCount,
        overlap: job.overlap,
        iteration: job.iteration,
        pipeline: DEFAULT_PIPELINE,
        inputVideoKey: scene.inputVideoKey,
        keyframeSetId: job.keyframeSetId,
        keyframesPrefix: job.keyframeSet?.storagePrefix ?? null,
        sourceSfmPrefix
      });
      await markStageRunSubmitted(stageRun.id, submittedJobId);
    } catch (submitErr) {
      await markStageRunSubmitFailed(stageRun?.id, submitErr);
      await prisma.jobs.update({
        where: {
          id: job.id
        },
        data: {
          status: "WAITING_GS",
          stage: "SFM_DONE",
          errorMessage: truncateErrorMessage(submitErr)
        }
      });
      console.error(submitErr);
      return sendApiError(res, req, 500, "INTERNAL_ERROR", "GS 작업 제출 실패");
    }

    const updated = await prisma.jobs.update({
      where: {
        id: job.id
      },
      data: {
        gsBatchJobId: submittedJobId,
        pipeline: DEFAULT_PIPELINE,
        status: "RUNNING",
        stage: "GS_TRAINING",
        progressPercent: 66,
        errorMessage: null,
        endedAt: null
      },
      select: jobBaseSelect
    });
    const keys = getMetaKeys(sceneId, updated.id);

    return res.status(202).json({
      jobId: toResponseId(updated.id),
      sceneId: toResponseId(scene.id),
      pipeline: DEFAULT_PIPELINE,
      status: "processing",
      batchJobId: submittedJobId,
      runner,
      sourceSfmPrefix,
      progressKey: keys.progressKey,
      statusKey: keys.statusKey
    });
  } catch (err) {
    console.error(err);
    return sendApiError(res, req, 500, "INTERNAL_ERROR", "GS 실행 실패");
  }
}

/**
 * Scene별 Job 진행률 조회
 * GET /api/v1/scenes/:sceneId/jobs/:jobId/progress
 */
export async function getSceneJobProgress(req, res) {
  try {
    const userId = parseBigInt(req.user?.id);
    if (userId === null) {
      return sendApiError(
        res,
        req,
        401,
        "UNAUTHORIZED",
        "세션 사용자 정보가 유효하지 않습니다."
      );
    }

    const sceneId = parseBigInt(req.params?.sceneId);
    const jobId = parseBigInt(req.params?.jobId);
    if (sceneId === null || jobId === null) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "sceneId, jobId는 숫자여야 합니다."
      );
    }

    const loadedScene = await loadOwnedScene(sceneId, userId);
    if (loadedScene.error) {
      return sendApiError(
        res,
        req,
        loadedScene.error.status,
        loadedScene.error.code,
        loadedScene.error.message
      );
    }

    const job = await loadOwnedJob(sceneId, jobId);
    if (!job) {
      return sendApiError(
        res,
        req,
        404,
        "JOB_NOT_FOUND",
        "job을 찾을 수 없습니다."
      );
    }

    const readModel = await buildJobReadModel(job, {
      bucketName: process.env.S3_BUCKET_NAME
    });
    const postId = job.post ? toResponseId(job.post.id) : null;

    return res.status(200).json({
      jobId: toResponseId(job.id),
      sceneId: toResponseId(job.sceneId),
      status: readModel.status,
      stage: readModel.stage,
      progress: readModel.progress,
      detail: readModel.detail,
      updatedAt: readModel.updatedAt,
      metrics: readModel.metrics,
      viewerReady: readModel.viewerReady,
      viewerKind: readModel.viewerKind,
      canRunGs: readModel.status === "waiting_gs" && Boolean(readModel.outputs.sfmResultKey),
      postable: readModel.postable,
      alreadyPosted: postId !== null,
      postId
    });
  } catch (err) {
    console.error(err);
    return sendApiError(
      res,
      req,
      500,
      "INTERNAL_ERROR",
      "scene job 진행률 조회 실패"
    );
  }
}

/**
 * Scene별 Job 최종 상태 조회
 * GET /api/v1/scenes/:sceneId/jobs/:jobId/status
 */
export async function getSceneJobStatus(req, res) {
  try {
    const userId = parseBigInt(req.user?.id);
    if (userId === null) {
      return sendApiError(
        res,
        req,
        401,
        "UNAUTHORIZED",
        "세션 사용자 정보가 유효하지 않습니다."
      );
    }

    const sceneId = parseBigInt(req.params?.sceneId);
    const jobId = parseBigInt(req.params?.jobId);
    if (sceneId === null || jobId === null) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "sceneId, jobId는 숫자여야 합니다."
      );
    }

    const loadedScene = await loadOwnedScene(sceneId, userId);
    if (loadedScene.error) {
      return sendApiError(
        res,
        req,
        loadedScene.error.status,
        loadedScene.error.code,
        loadedScene.error.message
      );
    }

    const job = await loadOwnedJob(sceneId, jobId);
    if (!job) {
      return sendApiError(
        res,
        req,
        404,
        "JOB_NOT_FOUND",
        "job을 찾을 수 없습니다."
      );
    }

    const readModel = await buildJobReadModel(job, {
      bucketName: process.env.S3_BUCKET_NAME
    });
    const postId = job.post ? toResponseId(job.post.id) : null;

    return res.status(200).json({
      jobId: toResponseId(job.id),
      sceneId: toResponseId(job.sceneId),
      pipeline: job.pipeline,
      status: readModel.status,
      outputs: readModel.outputs,
      viewerReady: readModel.viewerReady,
      viewerKind: readModel.viewerKind,
      canRunGs: readModel.status === "waiting_gs" && Boolean(readModel.outputs.sfmResultKey),
      postable: readModel.postable,
      alreadyPosted: postId !== null,
      postId,
      errorSummary: readModel.status === "failed" ? readModel.detail : null,
      updatedAt: readModel.updatedAt,
      finishedAt: readModel.finishedAt
    });
  } catch (err) {
    console.error(err);
    return sendApiError(
      res,
      req,
      500,
      "INTERNAL_ERROR",
      "scene job 상태 조회 실패"
    );
  }
}

/**
 * Job 뷰어 연결 정보 조회
 * GET /api/v1/jobs/:jobId/viewer
 */
export async function getJobViewer(req, res) {
  try {
    const userId = parseBigInt(req.user?.id);
    const jobId = parseBigInt(req.params?.jobId);
    if (jobId === null) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "jobId는 숫자여야 합니다."
      );
    }

    const job = await prisma.jobs.findUnique({
      where: {
        id: jobId
      },
      select: jobBaseSelect
    });

    if (!job) {
      return sendApiError(
        res,
        req,
        404,
        "JOB_NOT_FOUND",
        "job을 찾을 수 없습니다."
      );
    }

    const isOwner = userId !== null && job.scene.userId === userId;
    const isPublishedPost = job.post?.status === "PUBLISHED";
    if (!isOwner && !isPublishedPost) {
      return sendApiError(
        res,
        req,
        403,
        "FORBIDDEN",
        "접근 권한이 없습니다."
      );
    }

    const readModel = await buildJobReadModel(job, {
      bucketName: process.env.S3_BUCKET_NAME,
      viewerKind: req.query?.view ?? req.query?.viewerKind ?? req.query?.kind
    });
    const postId = job.post ? toResponseId(job.post.id) : null;
    const thumbnail = await buildThumbnailSummary({
      bucketName: process.env.S3_BUCKET_NAME,
      post: job.post,
      job,
      scene: job.scene
    });

    return res.status(200).json({
      jobId: toResponseId(job.id),
      sceneId: toResponseId(job.sceneId),
      pipeline: job.pipeline,
      status: readModel.status,
      viewerReady: readModel.viewerReady,
      viewerKind: readModel.viewerKind,
      postable: readModel.postable,
      isOwner,
      alreadyPosted: postId !== null,
      postId,
      thumbnailUrl: thumbnail.thumbnailUrl,
      thumbnailUpdatedAt: thumbnail.thumbnailUpdatedAt,
      format: VIEWER_FORMAT,
      resultUrl: readModel.resultUrl,
      file: readModel.file,
      updatedAt: readModel.updatedAt
    });
  } catch (err) {
    console.error(err);
    return sendApiError(
      res,
      req,
      500,
      "INTERNAL_ERROR",
      "job viewer 정보 조회 실패"
    );
  }
}
