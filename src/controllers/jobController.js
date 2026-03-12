import { prisma } from "../db.config.js";
import { sendApiError } from "../utils/apiError.js";
import {
  buildJobReadModel,
  buildThumbnailSummary,
  DEFAULT_PIPELINE,
  getMetaKeys,
  VIEWER_FORMAT
} from "../utils/jobPresentation.js";

const DEFAULT_LIMIT = 20;
const MAX_LIMIT = 50;

const jobBaseSelect = {
  id: true,
  sceneId: true,
  status: true,
  stage: true,
  progressPercent: true,
  errorMessage: true,
  batchJobId: true,
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
      thumbnailKey: true
    }
  }
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

function truncateErrorMessage(error) {
  const message =
    typeof error?.message === "string" && error.message.trim().length > 0
      ? error.message.trim()
      : "처리 실패";

  return message.slice(0, 255);
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
      title: true,
      status: true,
      uploadId: true,
      inputVideoKey: true,
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
    status: readModel.status,
    stage: readModel.stage,
    progress: readModel.progress,
    imageCount: job.imageCount,
    overlap: job.overlap,
    iteration: job.iteration,
    createdAt: job.createdAt.toISOString(),
    updatedAt: readModel.updatedAt,
    finishedAt: readModel.finishedAt,
    errorMessage: readModel.status === "failed" ? readModel.detail : null,
    viewerReady: readModel.viewerReady,
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
      pipeline,
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
    if (!scene.uploadId || !scene.inputVideoKey) {
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
        pipeline,
        imageCount,
        overlap,
        iteration,
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
      pipeline,
      imageCount,
      overlap,
      iteration,
      status: "queued",
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
      bucketName: process.env.S3_BUCKET_NAME
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
