import { GetObjectCommand, HeadObjectCommand } from "@aws-sdk/client-s3";
import { prisma } from "../db.config.js";
import { sendApiError } from "../utils/apiError.js";
import { s3 } from "../utils/s3.js";
import { streamToString } from "../utils/stream.js";

const DEFAULT_PIPELINE = "3dgs";
const DEFAULT_LIMIT = 20;
const MAX_LIMIT = 50;
const READY_STATUS = "ready";
const VIEWER_FORMAT = "ply";
const TERMINAL_STATUSES = new Set(["ready", "failed", "canceled"]);

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

function parseNonNegativeInt(value) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return null;
  }

  return parsed;
}

function parsePipeline(value) {
  if (value === undefined || value === null || String(value).trim().length === 0) {
    return DEFAULT_PIPELINE;
  }

  const normalized = String(value).trim().toLowerCase();
  if (normalized !== DEFAULT_PIPELINE) {
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

function mapJobStatus(status) {
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

function pickSceneResultKey(scene) {
  const candidates = [scene.gaussianSplatKey, scene.meshKey, scene.sfmResultKey];
  for (const key of candidates) {
    if (typeof key === "string" && key.trim().length > 0) {
      return key.trim();
    }
  }
  return null;
}

function buildPublicS3Url(bucketName, key) {
  const region = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION;
  const normalizedKey = key.replace(/^\/+/, "");
  const encodedKey = normalizedKey
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");

  if (region) {
    return `https://${bucketName}.s3.${region}.amazonaws.com/${encodedKey}`;
  }

  return `https://${bucketName}.s3.amazonaws.com/${encodedKey}`;
}

function getMetaKeys(sceneId, jobId) {
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

function truncateErrorMessage(error) {
  const message =
    typeof error?.message === "string" && error.message.trim().length > 0
      ? error.message.trim()
      : "처리 실패";
  return message.slice(0, 255);
}

async function headObjectIfExists(bucketName, key) {
  try {
    return await s3.send(
      new HeadObjectCommand({
        Bucket: bucketName,
        Key: key
      })
    );
  } catch (err) {
    const statusCode = err?.$metadata?.httpStatusCode;
    const errorName = err?.name;
    if (statusCode === 404 || errorName === "NotFound" || errorName === "NoSuchKey") {
      return null;
    }
    throw err;
  }
}

async function readJsonObjectFromS3(bucketName, key) {
  try {
    const result = await s3.send(
      new GetObjectCommand({
        Bucket: bucketName,
        Key: key
      })
    );

    if (!result.Body) {
      return null;
    }

    const raw = await streamToString(result.Body);
    if (typeof raw !== "string" || raw.trim().length === 0) {
      return null;
    }

    const parsed = JSON.parse(raw);
    if (!isPlainObject(parsed)) {
      return null;
    }

    return parsed;
  } catch (err) {
    const statusCode = err?.$metadata?.httpStatusCode;
    const errorName = err?.name;
    if (statusCode === 404 || errorName === "NotFound" || errorName === "NoSuchKey") {
      return null;
    }
    throw err;
  }
}

async function loadJobMetaFromS3(bucketName, sceneId, jobId) {
  const keys = getMetaKeys(sceneId, jobId);
  if (!bucketName) {
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

async function submitBatchJobToAws({
  sceneId,
  uploadId,
  jobId,
  imageCount,
  overlap,
  iteration,
  pipeline,
  bucketName
}) {
  const jobQueue = process.env.BATCH_JOB_QUEUE;
  const jobDefinition = process.env.BATCH_JOB_DEFINITION;
  const region = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION;

  if (!bucketName) {
    throw new Error("S3_BUCKET_NAME 환경변수가 필요합니다.");
  }

  if (!jobQueue) {
    throw new Error("BATCH_JOB_QUEUE 환경변수가 필요합니다.");
  }

  if (!jobDefinition) {
    throw new Error("BATCH_JOB_DEFINITION 환경변수가 필요합니다.");
  }

  const batchSdk = await import("@aws-sdk/client-batch").catch(() => null);
  if (!batchSdk) {
    throw new Error("@aws-sdk/client-batch 패키지가 필요합니다.");
  }

  const { BatchClient, SubmitJobCommand } = batchSdk;
  const batchClient = new BatchClient(region ? { region } : {});

  const namePrefix = process.env.BATCH_JOB_NAME_PREFIX || "scene-job";
  const rawJobName = `${namePrefix}-${sceneId.toString()}-${jobId.toString()}`;
  const jobName = rawJobName.replace(/[^A-Za-z0-9-_]/g, "-").slice(0, 128);

  const retryAttempts = Number.parseInt(String(process.env.BATCH_RETRY_ATTEMPTS ?? ""), 10);
  const containerName =
    process.env.BATCH_CONTAINER_NAME ?? process.env.BATCH_ECS_CONTAINER_NAME;

  if (!containerName || containerName.trim().length === 0) {
    throw new Error(
      "BATCH_CONTAINER_NAME(또는 BATCH_ECS_CONTAINER_NAME) 환경변수가 필요합니다."
    );
  }

  const environmentOverrides = [
    {
      name: "S3_BUCKET",
      value: bucketName
    },
    {
      name: "SCENE_ID",
      value: sceneId.toString()
    },
    {
      name: "UPLOAD_ID",
      value: uploadId
    },
    {
      name: "JOB_ID",
      value: jobId.toString()
    },
    {
      name: "IMG",
      value: String(imageCount)
    },
    {
      name: "OVERLAP",
      value: String(overlap)
    },
    {
      name: "ITERS",
      value: String(iteration)
    },
    {
      name: "PIPELINE",
      value: pipeline
    }
  ];

  const submitInput = {
    jobName,
    jobQueue,
    jobDefinition,
    ecsPropertiesOverride: {
      taskProperties: [
        {
          containers: [
            {
              name: containerName.trim(),
              environment: environmentOverrides
            }
          ]
        }
      ]
    }
  };

  if (Number.isFinite(retryAttempts) && retryAttempts > 0) {
    submitInput.retryStrategy = {
      attempts: retryAttempts
    };
  }

  const command = new SubmitJobCommand(submitInput);
  const response = await batchClient.send(command);
  if (typeof response?.jobId !== "string" || response.jobId.trim().length === 0) {
    throw new Error("AWS Batch jobId를 받지 못했습니다.");
  }

  return response.jobId.trim();
}

async function loadOwnedScene(sceneId, userId) {
  const scene = await prisma.scenes.findUnique({
    where: {
      id: sceneId
    },
    select: {
      id: true,
      userId: true,
      status: true,
      uploadId: true,
      inputVideoKey: true,
      gaussianSplatKey: true,
      meshKey: true,
      sfmResultKey: true
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
    select: {
      id: true,
      sceneId: true,
      status: true,
      stage: true,
      progressPercent: true,
      errorMessage: true,
      batchJobId: true,
      updatedAt: true
    }
  });
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

    const pipeline = parsePipeline(req.query?.pipeline);
    if (pipeline === null) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "pipeline은 3dgs만 지원합니다."
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
      select: {
        id: true,
        status: true,
        createdAt: true,
        endedAt: true,
        errorMessage: true
      }
    });

    const hasNext = foundJobs.length > limit;
    const jobs = hasNext ? foundJobs.slice(0, limit) : foundJobs;
    const sceneHasResult = pickSceneResultKey(loadedScene.scene) !== null;

    const nextCursor =
      hasNext && jobs.length > 0
        ? encodeCursor(jobs[jobs.length - 1].createdAt, jobs[jobs.length - 1].id)
        : null;

    return res.status(200).json({
      sceneId: toResponseId(loadedScene.scene.id),
      jobs: jobs.map((job) => {
        const status = mapJobStatus(job.status);
        return {
          id: toResponseId(job.id),
          pipeline,
          status,
          createdAt: job.createdAt.toISOString(),
          finishedAt: job.endedAt ? job.endedAt.toISOString() : null,
          errorMessage: job.errorMessage,
          resultExists: status === READY_STATUS && sceneHasResult
        };
      }),
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

/**
 * Scene별 Job 생성 + AWS Batch 제출
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

    const imageCount = parsePositiveInt(req.body?.imageCount);
    const overlap = parseNonNegativeInt(req.body?.overlap);
    const iteration = parsePositiveInt(req.body?.iteration);
    const pipeline = parsePipeline(req.body?.pipeline);

    if (
      imageCount === null ||
      overlap === null ||
      iteration === null ||
      pipeline === null
    ) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "imageCount, overlap, iteration, pipeline(3dgs) 값을 확인해주세요."
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
    if (!scene.uploadId || !scene.inputVideoKey || scene.status === "UPLOADING") {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "업로드 완료된 scene만 job 생성이 가능합니다."
      );
    }

    const created = await prisma.jobs.create({
      data: {
        sceneId,
        uploadId: scene.uploadId,
        status: "QUEUED",
        stage: "INSTANCE_CREATING",
        progressPercent: 0
      }
    });

    let awsBatchJobId = null;
    try {
      awsBatchJobId = await submitBatchJobToAws({
        sceneId,
        uploadId: scene.uploadId,
        jobId: created.id,
        imageCount,
        overlap,
        iteration,
        pipeline,
        bucketName: process.env.S3_BUCKET_NAME
      });
    } catch (submitErr) {
      await prisma.jobs.update({
        where: {
          id: created.id
        },
        data: {
          status: "FAILED",
          errorMessage: truncateErrorMessage(submitErr),
          endedAt: new Date()
        }
      });

      console.error(submitErr);
      return sendApiError(
        res,
        req,
        500,
        "INTERNAL_ERROR",
        "AWS Batch 작업 제출 실패"
      );
    }

    const updated = await prisma.jobs.update({
      where: {
        id: created.id
      },
      data: {
        batchJobId: awsBatchJobId,
        status: "SUBMITTED"
      }
    });

    const keys = getMetaKeys(sceneId, updated.id);

    return res.status(201).json({
      jobId: toResponseId(updated.id),
      sceneId: toResponseId(scene.id),
      status: mapJobStatus(updated.status),
      batchJobId: awsBatchJobId,
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

    const bucketName = process.env.S3_BUCKET_NAME;
    const { progressDoc, statusDoc } = await loadJobMetaFromS3(
      bucketName,
      sceneId,
      jobId
    );

    const status = resolveStatus(job.status, progressDoc, statusDoc);
    const stage = resolveStage(job.stage, progressDoc, statusDoc);
    const progress = resolveProgress(job.progressPercent, status, progressDoc);
    const detail = resolveDetail(job.errorMessage, progressDoc, statusDoc);
    const updatedAt = resolveUpdatedAt(job.updatedAt, progressDoc, statusDoc);
    const metrics = resolveMetrics(progressDoc, statusDoc);

    return res.status(200).json({
      jobId: toResponseId(job.id),
      sceneId: toResponseId(job.sceneId),
      status,
      stage,
      progress,
      detail,
      updatedAt,
      metrics
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

    const bucketName = process.env.S3_BUCKET_NAME;
    const { keys, progressDoc, statusDoc } = await loadJobMetaFromS3(
      bucketName,
      sceneId,
      jobId
    );

    const status = resolveStatus(job.status, progressDoc, statusDoc);
    const updatedAt = resolveUpdatedAt(job.updatedAt, progressDoc, statusDoc);
    const errorSummary = resolveDetail(job.errorMessage, progressDoc, statusDoc);

    let outputs = null;
    if (isPlainObject(statusDoc?.outputs)) {
      outputs = statusDoc.outputs;
    }

    if (status === READY_STATUS) {
      const resultKeyFromStatus =
        typeof statusDoc?.resultKey === "string" && statusDoc.resultKey.trim().length > 0
          ? statusDoc.resultKey.trim()
          : keys.resultKey;

      outputs = {
        ...(outputs ?? {}),
        resultKey: resultKeyFromStatus,
        resultUrl: bucketName ? buildPublicS3Url(bucketName, resultKeyFromStatus) : null
      };
    }

    return res.status(200).json({
      jobId: toResponseId(job.id),
      sceneId: toResponseId(job.sceneId),
      status,
      outputs,
      errorSummary: status === "failed" ? errorSummary : null,
      updatedAt
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
    if (userId === null) {
      return sendApiError(
        res,
        req,
        401,
        "UNAUTHORIZED",
        "세션 사용자 정보가 유효하지 않습니다."
      );
    }

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
      select: {
        id: true,
        sceneId: true,
        status: true,
        updatedAt: true,
        scene: {
          select: {
            id: true,
            userId: true,
            gaussianSplatKey: true,
            meshKey: true,
            sfmResultKey: true
          }
        }
      }
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

    if (job.scene.userId !== userId) {
      return sendApiError(
        res,
        req,
        403,
        "FORBIDDEN",
        "접근 권한이 없습니다."
      );
    }

    const bucketName = process.env.S3_BUCKET_NAME;
    const { keys, progressDoc, statusDoc } = await loadJobMetaFromS3(
      bucketName,
      job.sceneId,
      job.id
    );

    const status = resolveStatus(job.status, progressDoc, statusDoc);
    const response = {
      jobId: toResponseId(job.id),
      sceneId: toResponseId(job.sceneId),
      pipeline: DEFAULT_PIPELINE,
      status,
      format: VIEWER_FORMAT,
      resultUrl: null,
      file: null,
      updatedAt: resolveUpdatedAt(job.updatedAt, progressDoc, statusDoc)
    };

    if (status !== READY_STATUS) {
      return res.status(200).json(response);
    }

    if (!bucketName) {
      return sendApiError(
        res,
        req,
        500,
        "INTERNAL_ERROR",
        "S3 버킷 설정이 없습니다."
      );
    }

    const resultCandidates = [];
    const resultKeyFromStatus =
      typeof statusDoc?.resultKey === "string" && statusDoc.resultKey.trim().length > 0
        ? statusDoc.resultKey.trim()
        : null;
    if (resultKeyFromStatus) {
      resultCandidates.push(resultKeyFromStatus);
    }
    resultCandidates.push(keys.resultKey);

    const fallbackSceneKey = pickSceneResultKey(job.scene);
    if (fallbackSceneKey) {
      resultCandidates.push(fallbackSceneKey);
    }

    for (const candidate of resultCandidates) {
      const headResult = await headObjectIfExists(bucketName, candidate);
      if (!headResult) {
        continue;
      }

      return res.status(200).json({
        ...response,
        resultUrl: buildPublicS3Url(bucketName, candidate),
        file: buildFileInfo(headResult)
      });
    }

    return res.status(200).json(response);
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
