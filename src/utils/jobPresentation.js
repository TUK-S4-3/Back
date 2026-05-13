import { prisma } from "../db.config.js";
import {
  buildPublicStorageUrl,
  buildStorageGetUrl,
  headStorageObject,
  isLocalStorage,
  readJsonStorageObject
} from "./storage.js";

export const DEFAULT_PIPELINE = "3dgs";
export const READY_STATUS = "ready";
export const VIEWER_FORMAT = "ply";

const TERMINAL_STATUSES = new Set([READY_STATUS, "failed", "canceled"]);
const JOB_STAGE_VALUES = new Set([
  "INSTANCE_CREATING",
  "IMAGESET_BUILDING",
  "SFM_FEATURE",
  "SFM_MATCH",
  "SFM_MAPPER",
  "SFM",
  "UNDISTORT",
  "GS_TRAINING",
  "MESH_EXTRACTION",
  "FINALIZING",
  "UPLOADING",
  "DONE"
]);
const DEFAULT_THUMBNAIL_GET_EXPIRES_IN_SECONDS = 60 * 60;

function isPlainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function clamp(number, min, max) {
  return Math.min(Math.max(number, min), max);
}

function toIsoStringOrNull(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return null;
  }

  return date.toISOString();
}

function toDateOrNull(value) {
  const iso = toIsoStringOrNull(value);
  return iso ? new Date(iso) : null;
}

function normalizeStorageKey(value) {
  if (typeof value !== "string" || value.trim().length === 0) {
    return null;
  }

  return value.trim();
}

function normalizeApiStatus(value) {
  if (typeof value !== "string") {
    return null;
  }

  const normalized = value.trim().toLowerCase();
  switch (normalized) {
    case "queued":
    case "submitted":
    case "pending":
    case "runnable":
    case "starting":
      return "queued";
    case "running":
    case "processing":
      return "processing";
    case "succeeded":
    case "success":
    case "ready":
    case "done":
      return READY_STATUS;
    case "failed":
    case "error":
      return "failed";
    case "cancelled":
    case "canceled":
      return "canceled";
    default:
      return null;
  }
}

export function mapJobStatus(status) {
  switch (status) {
    case "QUEUED":
    case "SUBMITTED":
      return "queued";
    case "RUNNING":
      return "processing";
    case "SUCCEEDED":
      return READY_STATUS;
    case "FAILED":
      return "failed";
    case "CANCELLED":
      return "canceled";
    default:
      return "processing";
  }
}

function resolveUpdatedAt(fallbackDate, progressDoc, statusDoc) {
  const candidates = [progressDoc?.updatedAt, statusDoc?.updatedAt];
  for (const candidate of candidates) {
    const iso = toIsoStringOrNull(candidate);
    if (iso) {
      return iso;
    }
  }

  return fallbackDate.toISOString();
}

function resolveFinishedAt(fallbackDate, resolvedStatus, updatedAt, progressDoc, statusDoc) {
  if (fallbackDate) {
    return fallbackDate.toISOString();
  }

  const candidates = [
    statusDoc?.endedAt,
    statusDoc?.finishedAt,
    progressDoc?.endedAt,
    progressDoc?.finishedAt
  ];

  for (const candidate of candidates) {
    const iso = toIsoStringOrNull(candidate);
    if (iso) {
      return iso;
    }
  }

  return TERMINAL_STATUSES.has(resolvedStatus) ? updatedAt : null;
}

function resolveStartedAt(fallbackDate, resolvedStatus, progressDoc, statusDoc) {
  if (fallbackDate) {
    return fallbackDate.toISOString();
  }

  if (resolvedStatus !== "processing" && !TERMINAL_STATUSES.has(resolvedStatus)) {
    return null;
  }

  const candidates = [
    statusDoc?.startedAt,
    progressDoc?.startedAt,
    progressDoc?.updatedAt,
    statusDoc?.updatedAt
  ];

  for (const candidate of candidates) {
    const iso = toIsoStringOrNull(candidate);
    if (iso) {
      return iso;
    }
  }

  return null;
}

function resolveStage(fallbackStage, progressDoc, statusDoc) {
  const fromProgress = progressDoc?.stage;
  if (typeof fromProgress === "string" && fromProgress.trim().length > 0) {
    return fromProgress.trim();
  }

  const fromStatus = statusDoc?.stage;
  if (typeof fromStatus === "string" && fromStatus.trim().length > 0) {
    return fromStatus.trim();
  }

  if (typeof fallbackStage === "string" && fallbackStage.trim().length > 0) {
    return fallbackStage.trim();
  }

  return null;
}

function resolveDetail(fallbackErrorMessage, progressDoc, statusDoc) {
  const fromProgress = progressDoc?.detail;
  if (typeof fromProgress === "string" && fromProgress.trim().length > 0) {
    return fromProgress.trim();
  }

  const fromStatus = statusDoc?.detail;
  if (typeof fromStatus === "string" && fromStatus.trim().length > 0) {
    return fromStatus.trim();
  }

  const fromErrorSummary = statusDoc?.errorSummary;
  if (typeof fromErrorSummary === "string" && fromErrorSummary.trim().length > 0) {
    return fromErrorSummary.trim();
  }

  if (
    typeof fallbackErrorMessage === "string" &&
    fallbackErrorMessage.trim().length > 0
  ) {
    return fallbackErrorMessage.trim();
  }

  return null;
}

function resolveMetrics(progressDoc, statusDoc) {
  if (isPlainObject(progressDoc?.metrics)) {
    return progressDoc.metrics;
  }

  if (isPlainObject(statusDoc?.metrics)) {
    return statusDoc.metrics;
  }

  return {};
}

function resolveStatus(dbStatus, progressDoc, statusDoc) {
  return (
    normalizeApiStatus(statusDoc?.status) ??
    normalizeApiStatus(progressDoc?.status) ??
    mapJobStatus(dbStatus)
  );
}

function resolveProgress(dbProgressPercent, resolvedStatus, progressDoc) {
  const progressFromDoc = Number(progressDoc?.progress);
  if (Number.isFinite(progressFromDoc)) {
    return clamp(progressFromDoc, 0, 1);
  }

  const fromDb = clamp(Number(dbProgressPercent ?? 0) / 100, 0, 1);
  if (TERMINAL_STATUSES.has(resolvedStatus)) {
    return 1;
  }

  return fromDb;
}

function resolveUrlVersion(value) {
  const date = toDateOrNull(value);
  if (!date) {
    return null;
  }

  return String(date.getTime());
}

export function buildPublicS3Url(bucketName, key) {
  return buildPublicStorageUrl(bucketName, key);
}

export function buildVersionedPublicS3Url(bucketName, key, updatedAt) {
  const baseUrl = buildPublicS3Url(bucketName, key);
  if (!baseUrl) {
    return null;
  }

  const version = resolveUrlVersion(updatedAt);
  return version ? `${baseUrl}?v=${encodeURIComponent(version)}` : baseUrl;
}

function getThumbnailGetExpiresInSeconds() {
  const raw = Number.parseInt(
    String(process.env.THUMBNAIL_GET_SIGNED_URL_EXPIRES_IN_SECONDS ?? ""),
    10
  );

  if (Number.isFinite(raw) && raw > 0) {
    return raw;
  }

  return DEFAULT_THUMBNAIL_GET_EXPIRES_IN_SECONDS;
}

export async function buildSignedGetObjectUrl(bucketName, key) {
  const normalizedKey = normalizeStorageKey(key);
  if (!normalizedKey) {
    return null;
  }

  return buildStorageGetUrl(bucketName, normalizedKey, {
    expiresIn: getThumbnailGetExpiresInSeconds()
  });
}

async function buildThumbnailPayload(bucketName, key, updatedAt) {
  const thumbnailKey = normalizeStorageKey(key);
  const thumbnailUpdatedAt = toIsoStringOrNull(updatedAt);

  return {
    thumbnailKey,
    thumbnailUpdatedAt,
    thumbnailUrl: await buildSignedGetObjectUrl(bucketName, thumbnailKey)
  };
}

export async function buildThumbnailSummary({
  bucketName = process.env.S3_BUCKET_NAME ?? null,
  post = null,
  job = null,
  scene = null
} = {}) {
  if (post?.thumbnailKey) {
    return buildThumbnailPayload(
      bucketName,
      post.thumbnailKey,
      post.thumbnailUpdatedAt ?? post.updatedAt ?? null
    );
  }

  if (job?.thumbnailKey) {
    return buildThumbnailPayload(bucketName, job.thumbnailKey, job.updatedAt ?? null);
  }

  if (scene?.thumbnailKey) {
    return buildThumbnailPayload(bucketName, scene.thumbnailKey, scene.updatedAt ?? null);
  }

  return {
    thumbnailKey: null,
    thumbnailUpdatedAt: null,
    thumbnailUrl: null
  };
}

export function getMetaKeys(sceneId, jobId) {
  const sceneIdText = sceneId.toString();
  const jobIdText = jobId.toString();

  return {
    progressKey: `scenes/${sceneIdText}/meta/${jobIdText}/progress.json`,
    statusKey: `scenes/${sceneIdText}/meta/${jobIdText}/status.json`,
    resultKey: `scenes/${sceneIdText}/gs/${jobIdText}/result.ply`
  };
}

function buildFileInfo(headResult) {
  if (!headResult) {
    return null;
  }

  const etag =
    typeof headResult.ETag === "string"
      ? headResult.ETag.replaceAll('"', "")
      : null;

  return {
    contentLength:
      typeof headResult.ContentLength === "number"
        ? headResult.ContentLength
        : null,
    etag,
    acceptRanges: String(headResult.AcceptRanges ?? "").toLowerCase() === "bytes"
  };
}

async function headObjectIfExists(bucketName, key) {
  return headStorageObject(bucketName, key);
}

async function readJsonObjectFromS3(bucketName, key) {
  return readJsonStorageObject(bucketName, key);
}

export async function loadJobMetaFromS3(bucketName, sceneId, jobId) {
  const keys = getMetaKeys(sceneId, jobId);
  if (!bucketName && !isLocalStorage()) {
    return {
      keys,
      progressDoc: null,
      statusDoc: null
    };
  }

  const [progressDoc, statusDoc] = await Promise.all([
    readJsonObjectFromS3(bucketName, keys.progressKey),
    readJsonObjectFromS3(bucketName, keys.statusKey)
  ]);

  return {
    keys,
    progressDoc,
    statusDoc
  };
}

function pickSceneResultKey(scene) {
  const candidates = [scene?.gaussianSplatKey, scene?.meshKey, scene?.sfmResultKey];
  for (const candidate of candidates) {
    const normalized = normalizeStorageKey(candidate);
    if (normalized) {
      return normalized;
    }
  }

  return null;
}

function resolveSceneFallbackKeys(scene) {
  return {
    resultKey: pickSceneResultKey(scene),
    gaussianSplatKey: normalizeStorageKey(scene?.gaussianSplatKey),
    meshKey: normalizeStorageKey(scene?.meshKey),
    sfmResultKey: normalizeStorageKey(scene?.sfmResultKey),
    thumbnailKey: normalizeStorageKey(scene?.thumbnailKey)
  };
}

function resolveOutputKeys(job, statusDoc, defaultResultKey) {
  const outputs = isPlainObject(statusDoc?.outputs) ? statusDoc.outputs : {};
  const statusResultKey = normalizeStorageKey(statusDoc?.resultKey);
  const outputsResultKey = normalizeStorageKey(outputs.resultKey);
  const outputsGaussianKey = normalizeStorageKey(outputs.gaussianSplatKey);
  const defaultKey = normalizeStorageKey(defaultResultKey);

  const resultKey =
    statusResultKey ??
    outputsResultKey ??
    outputsGaussianKey ??
    normalizeStorageKey(job.gaussianSplatKey) ??
    defaultKey;

  return {
    resultKey,
    gaussianSplatKey:
      outputsGaussianKey ??
      statusResultKey ??
      outputsResultKey ??
      normalizeStorageKey(job.gaussianSplatKey) ??
      defaultKey,
    meshKey: normalizeStorageKey(outputs.meshKey) ?? normalizeStorageKey(job.meshKey),
    sfmResultKey:
      normalizeStorageKey(outputs.sfmResultKey) ?? normalizeStorageKey(job.sfmResultKey),
    thumbnailKey:
      normalizeStorageKey(outputs.thumbnailKey) ?? normalizeStorageKey(job.thumbnailKey)
  };
}

function dedupeKeys(values) {
  const seen = new Set();
  const result = [];

  for (const value of values) {
    const normalized = normalizeStorageKey(value);
    if (!normalized || seen.has(normalized)) {
      continue;
    }

    seen.add(normalized);
    result.push(normalized);
  }

  return result;
}

async function findViewerAsset(bucketName, resolvedStatus, outputKeys, sceneFallbackKeys) {
  if (resolvedStatus !== READY_STATUS || (!bucketName && !isLocalStorage())) {
    return null;
  }

  const candidates = dedupeKeys([
    outputKeys.resultKey,
    outputKeys.gaussianSplatKey,
    outputKeys.meshKey,
    outputKeys.sfmResultKey,
    sceneFallbackKeys.resultKey,
    sceneFallbackKeys.gaussianSplatKey,
    sceneFallbackKeys.meshKey,
    sceneFallbackKeys.sfmResultKey
  ]);

  for (const candidate of candidates) {
    const headResult = await headObjectIfExists(bucketName, candidate);
    if (!headResult) {
      continue;
    }

    return {
      key: candidate,
      headResult
    };
  }

  return null;
}

function deriveDbStatus(apiStatus, currentDbStatus) {
  switch (apiStatus) {
    case "processing":
      return "RUNNING";
    case READY_STATUS:
      return "SUCCEEDED";
    case "failed":
      return "FAILED";
    case "canceled":
      return "CANCELLED";
    default:
      return currentDbStatus;
  }
}

async function syncJobState({
  job,
  resolvedStatus,
  resolvedStage,
  resolvedDetail,
  progressPercent,
  outputKeys,
  startedAt,
  finishedAt
}) {
  const data = {};
  const nextDbStatus = deriveDbStatus(resolvedStatus, job.status);

  if (nextDbStatus !== job.status) {
    data.status = nextDbStatus;
  }

  if (JOB_STAGE_VALUES.has(resolvedStage) && resolvedStage !== job.stage) {
    data.stage = resolvedStage;
  }

  if (Number.isFinite(progressPercent) && progressPercent !== job.progressPercent) {
    data.progressPercent = progressPercent;
  }

  if (resolvedStatus === "failed" && resolvedDetail && resolvedDetail !== job.errorMessage) {
    data.errorMessage = resolvedDetail.slice(0, 255);
  }

  if (outputKeys.sfmResultKey && outputKeys.sfmResultKey !== job.sfmResultKey) {
    data.sfmResultKey = outputKeys.sfmResultKey;
  }

  if (
    outputKeys.gaussianSplatKey &&
    outputKeys.gaussianSplatKey !== job.gaussianSplatKey
  ) {
    data.gaussianSplatKey = outputKeys.gaussianSplatKey;
  }

  if (outputKeys.meshKey && outputKeys.meshKey !== job.meshKey) {
    data.meshKey = outputKeys.meshKey;
  }

  if (outputKeys.thumbnailKey && outputKeys.thumbnailKey !== job.thumbnailKey) {
    data.thumbnailKey = outputKeys.thumbnailKey;
  }

  if (!job.startedAt && startedAt) {
    const parsedStartedAt = toDateOrNull(startedAt);
    if (parsedStartedAt) {
      data.startedAt = parsedStartedAt;
    }
  }

  if (!job.endedAt && finishedAt && TERMINAL_STATUSES.has(resolvedStatus)) {
    const parsedEndedAt = toDateOrNull(finishedAt);
    if (parsedEndedAt) {
      data.endedAt = parsedEndedAt;
    }
  }

  if (Object.keys(data).length === 0) {
    return;
  }

  await prisma.jobs.update({
    where: {
      id: job.id
    },
    data
  });
}

function buildOutputsResponse(bucketName, outputKeys, sceneFallbackKeys, viewerAsset) {
  const gaussianSplatKey =
    outputKeys.gaussianSplatKey ?? sceneFallbackKeys.gaussianSplatKey ?? null;
  const meshKey = outputKeys.meshKey ?? sceneFallbackKeys.meshKey ?? null;
  const sfmResultKey = outputKeys.sfmResultKey ?? sceneFallbackKeys.sfmResultKey ?? null;
  const thumbnailKey = outputKeys.thumbnailKey ?? sceneFallbackKeys.thumbnailKey ?? null;
  const resultKey =
    viewerAsset?.key ??
    outputKeys.resultKey ??
    gaussianSplatKey ??
    sceneFallbackKeys.resultKey ??
    null;

  return {
    resultKey,
    resultUrl: buildPublicS3Url(bucketName, resultKey),
    gaussianSplatKey,
    gaussianSplatUrl: buildPublicS3Url(bucketName, gaussianSplatKey),
    meshKey,
    meshUrl: buildPublicS3Url(bucketName, meshKey),
    sfmResultKey,
    sfmResultUrl: buildPublicS3Url(bucketName, sfmResultKey),
    thumbnailKey,
    thumbnailUrl: buildPublicS3Url(bucketName, thumbnailKey)
  };
}

export async function buildJobReadModel(job, options = {}) {
  const bucketName = options.bucketName ?? process.env.S3_BUCKET_NAME ?? null;
  const { keys, progressDoc, statusDoc } = await loadJobMetaFromS3(
    bucketName,
    job.sceneId,
    job.id
  );

  const status = resolveStatus(job.status, progressDoc, statusDoc);
  const stage = resolveStage(job.stage, progressDoc, statusDoc);
  const progress = resolveProgress(job.progressPercent, status, progressDoc);
  const progressPercent = clamp(Math.round(progress * 100), 0, 100);
  const detail = resolveDetail(job.errorMessage, progressDoc, statusDoc);
  const updatedAt = resolveUpdatedAt(job.updatedAt, progressDoc, statusDoc);
  const startedAt = resolveStartedAt(job.startedAt, status, progressDoc, statusDoc);
  const finishedAt = resolveFinishedAt(job.endedAt, status, updatedAt, progressDoc, statusDoc);
  const metrics = resolveMetrics(progressDoc, statusDoc);

  const outputKeys = resolveOutputKeys(job, statusDoc, keys.resultKey);
  const sceneFallbackKeys = resolveSceneFallbackKeys(job.scene);

  if (options.syncDb !== false) {
    await syncJobState({
      job,
      resolvedStatus: status,
      resolvedStage: stage,
      resolvedDetail: detail,
      progressPercent,
      outputKeys,
      startedAt,
      finishedAt
    });
  }

  const viewerAsset = await findViewerAsset(
    bucketName,
    status,
    outputKeys,
    sceneFallbackKeys
  );
  const viewerReady = viewerAsset !== null;
  const outputs = buildOutputsResponse(bucketName, outputKeys, sceneFallbackKeys, viewerAsset);

  return {
    keys,
    progressDoc,
    statusDoc,
    status,
    stage,
    progress,
    progressPercent,
    detail,
    metrics,
    updatedAt,
    startedAt,
    finishedAt,
    outputs,
    viewerReady,
    postable: viewerReady && !job.post,
    file: viewerAsset ? buildFileInfo(viewerAsset.headResult) : null,
    resultUrl: viewerAsset ? buildPublicS3Url(bucketName, viewerAsset.key) : null
  };
}
