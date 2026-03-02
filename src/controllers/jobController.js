import { HeadObjectCommand } from "@aws-sdk/client-s3";
import { prisma } from "../db.config.js";
import { sendApiError } from "../utils/apiError.js";
import { s3 } from "../utils/s3.js";

const DEFAULT_PIPELINE = "3dgs";
const DEFAULT_LIMIT = 20;
const MAX_LIMIT = 50;
const READY_STATUS = "ready";
const VIEWER_FORMAT = "ply";

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

    const scene = await prisma.scenes.findUnique({
      where: {
        id: sceneId
      },
      select: {
        id: true,
        userId: true,
        gaussianSplatKey: true,
        meshKey: true,
        sfmResultKey: true
      }
    });

    if (!scene) {
      return sendApiError(
        res,
        req,
        404,
        "SCENE_NOT_FOUND",
        "scene을 찾을 수 없습니다."
      );
    }

    if (scene.userId !== userId) {
      return sendApiError(
        res,
        req,
        403,
        "FORBIDDEN",
        "접근 권한이 없습니다."
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
    const sceneHasResult = pickSceneResultKey(scene) !== null;

    const nextCursor =
      hasNext && jobs.length > 0
        ? encodeCursor(jobs[jobs.length - 1].createdAt, jobs[jobs.length - 1].id)
        : null;

    return res.status(200).json({
      sceneId: toResponseId(scene.id),
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

    const status = mapJobStatus(job.status);
    const response = {
      jobId: toResponseId(job.id),
      sceneId: toResponseId(job.sceneId),
      pipeline: DEFAULT_PIPELINE,
      status,
      format: VIEWER_FORMAT,
      resultUrl: null,
      file: null,
      updatedAt: job.updatedAt.toISOString()
    };

    if (status !== READY_STATUS) {
      return res.status(200).json(response);
    }

    const resultKey = pickSceneResultKey(job.scene);
    if (!resultKey) {
      return res.status(200).json(response);
    }

    const bucketName = process.env.S3_BUCKET_NAME;
    if (!bucketName) {
      return sendApiError(
        res,
        req,
        500,
        "INTERNAL_ERROR",
        "S3 버킷 설정이 없습니다."
      );
    }

    const headResult = await headObjectIfExists(bucketName, resultKey);
    if (!headResult) {
      return res.status(200).json(response);
    }

    return res.status(200).json({
      ...response,
      resultUrl: buildPublicS3Url(bucketName, resultKey),
      file: buildFileInfo(headResult)
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
