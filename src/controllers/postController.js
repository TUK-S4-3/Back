import { HeadObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { v4 as uuid } from "uuid";
import { prisma } from "../db.config.js";
import { sendApiError } from "../utils/apiError.js";
import {
  buildJobReadModel,
  buildThumbnailSummary
} from "../utils/jobPresentation.js";
import { s3 } from "../utils/s3.js";

const DEFAULT_PAGE = 1;
const DEFAULT_PAGE_SIZE = 20;
const MAX_PAGE_SIZE = 50;
const THUMBNAIL_CONTENT_TYPE = "image/jpeg";
const THUMBNAIL_MAX_BYTES = 10 * 1024 * 1024;
const THUMBNAIL_PRESIGNED_URL_EXPIRES_IN_SECONDS = 60 * 5;

const postBaseSelect = {
  id: true,
  userId: true,
  title: true,
  status: true,
  likeCount: true,
  downloadCount: true,
  shareUuid: true,
  thumbnailKey: true,
  thumbnailUpdatedAt: true,
  createdAt: true,
  updatedAt: true,
  user: {
    select: {
      id: true,
      nickname: true,
      profileImageUrl: true
    }
  },
  job: {
    select: {
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
          status: true
        }
      },
      scene: {
        select: {
          id: true,
          userId: true,
          title: true,
          updatedAt: true,
          gaussianSplatKey: true,
          meshKey: true,
          sfmResultKey: true,
          thumbnailKey: true
        }
      }
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

function parsePage(value) {
  if (value === undefined) {
    return DEFAULT_PAGE;
  }

  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isFinite(parsed) || parsed < 1) {
    return null;
  }

  return parsed;
}

function parsePageSize(value) {
  if (value === undefined) {
    return DEFAULT_PAGE_SIZE;
  }

  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isFinite(parsed) || parsed < 1 || parsed > MAX_PAGE_SIZE) {
    return null;
  }

  return parsed;
}

function toResponseId(value) {
  const numeric = Number(value);
  if (Number.isSafeInteger(numeric)) {
    return numeric;
  }

  return value.toString();
}

function normalizeTitle(value, fallback) {
  if (value === undefined || value === null || String(value).trim().length === 0) {
    return fallback;
  }

  const normalized = String(value).trim();
  if (normalized.length > 100) {
    return null;
  }

  return normalized;
}

function buildPostViewerApiPath(postId) {
  return `/api/v1/posts/${toResponseId(postId)}/viewer`;
}

function normalizeStorageKey(value) {
  if (typeof value !== "string" || value.trim().length === 0) {
    return null;
  }

  return value.trim();
}

function buildPostThumbnailKey(sceneId, jobId) {
  return `scenes/${sceneId}/thumb/${jobId}/thumbnail.jpg`;
}

async function buildPostThumbnailSummary(post) {
  return buildThumbnailSummary({
    bucketName: process.env.S3_BUCKET_NAME,
    post,
    job: post.job,
    scene: post.job?.scene ?? null
  });
}

async function loadOwnedPublishedPost(postId, userId) {
  const post = await prisma.posts.findFirst({
    where: {
      id: postId,
      status: "PUBLISHED"
    },
    select: {
      id: true,
      userId: true,
      jobId: true,
      thumbnailKey: true,
      thumbnailUpdatedAt: true,
      updatedAt: true,
      job: {
        select: {
          id: true,
          sceneId: true
        }
      }
    }
  });

  if (!post) {
    return {
      post: null,
      error: {
        status: 404,
        code: "POST_NOT_FOUND",
        message: "게시물을 찾을 수 없습니다."
      }
    };
  }

  if (post.userId !== userId) {
    return {
      post: null,
      error: {
        status: 403,
        code: "FORBIDDEN",
        message: "접근 권한이 없습니다."
      }
    };
  }

  return {
    post,
    error: null
  };
}

async function buildPostListItem(post) {
  const thumbnail = await buildPostThumbnailSummary(post);

  return {
    id: toResponseId(post.id),
    postId: toResponseId(post.id),
    title: post.title,
    authorName: post.user.nickname ?? null,
    authorProfileImageUrl: post.user.profileImageUrl ?? null,
    createdAt: post.createdAt.toISOString(),
    likeCount: Number(post.likeCount ?? 0),
    downloadCount: Number(post.downloadCount ?? 0),
    thumbnailUrl: thumbnail.thumbnailUrl,
    jobId: toResponseId(post.job.id),
    sceneId: toResponseId(post.job.sceneId)
  };
}

async function buildPostDetail(post) {
  const readModel = await buildJobReadModel(post.job, {
    bucketName: process.env.S3_BUCKET_NAME
  });
  const thumbnail = await buildPostThumbnailSummary(post);

  return {
    id: toResponseId(post.id),
    postId: toResponseId(post.id),
    title: post.title,
    shareUuid: post.shareUuid,
    createdAt: post.createdAt.toISOString(),
    updatedAt: post.updatedAt.toISOString(),
    thumbnailUrl: thumbnail.thumbnailUrl,
    thumbnailUpdatedAt: thumbnail.thumbnailUpdatedAt,
    likeCount: Number(post.likeCount ?? 0),
    downloadCount: Number(post.downloadCount ?? 0),
    author: {
      id: toResponseId(post.user.id),
      nickname: post.user.nickname ?? null,
      profileImageUrl: post.user.profileImageUrl ?? null
    },
    authorName: post.user.nickname ?? null,
    authorProfileImageUrl: post.user.profileImageUrl ?? null,
    job: {
      id: toResponseId(post.job.id),
      sceneId: toResponseId(post.job.sceneId),
      pipeline: post.job.pipeline,
      status: readModel.status,
      imageCount: post.job.imageCount,
      overlap: post.job.overlap,
      iteration: post.job.iteration,
      viewerReady: readModel.viewerReady,
      alreadyPosted: Boolean(post.job.post),
      postId: post.job.post ? toResponseId(post.job.post.id) : null,
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
      outputs: readModel.outputs,
      createdAt: post.job.createdAt.toISOString(),
      finishedAt: readModel.finishedAt
    },
    viewerPath: buildPostViewerApiPath(post.id)
  };
}

/**
 * 게시물 생성
 * POST /api/v1/posts
 */
export async function createPost(req, res) {
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

    const jobId = parseBigInt(req.body?.jobId);
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
      select: postBaseSelect.job.select
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

    if (job.post) {
      return sendApiError(
        res,
        req,
        409,
        "POST_ALREADY_EXISTS",
        "이미 게시된 job입니다."
      );
    }

    const title = normalizeTitle(req.body?.title, job.scene.title ?? "Untitled Post");
    if (title === null) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "title은 100자 이하여야 합니다."
      );
    }

    const readModel = await buildJobReadModel(job, {
      bucketName: process.env.S3_BUCKET_NAME
    });
    const thumbnail = await buildThumbnailSummary({
      bucketName: process.env.S3_BUCKET_NAME,
      job,
      scene: job.scene ?? null
    });

    if (!readModel.viewerReady) {
      return sendApiError(
        res,
        req,
        400,
        "JOB_NOT_VIEWABLE",
        "viewerReady 상태의 job만 게시할 수 있습니다."
      );
    }

    const created = await prisma.posts.create({
      data: {
        userId,
        jobId,
        title,
        shareUuid: uuid()
      }
    });

    return res.status(201).json({
      postId: toResponseId(created.id),
      jobId: toResponseId(job.id),
      sceneId: toResponseId(job.sceneId),
      title: created.title,
      shareUuid: created.shareUuid,
      thumbnailUrl: thumbnail.thumbnailUrl,
      viewerPath: buildPostViewerApiPath(created.id),
      job: {
        id: toResponseId(job.id),
        sceneId: toResponseId(job.sceneId),
        thumbnailUrl: thumbnail.thumbnailUrl
      }
    });
  } catch (err) {
    console.error(err);
    return sendApiError(
      res,
      req,
      500,
      "INTERNAL_ERROR",
      "게시물 생성 실패"
    );
  }
}

/**
 * 게시물 목록 조회
 * GET /api/v1/posts?page=1&pageSize=20
 */
export async function listPosts(req, res) {
  try {
    const page = parsePage(req.query?.page);
    if (page === null) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "page는 1 이상의 정수여야 합니다."
      );
    }

    const pageSize = parsePageSize(req.query?.pageSize);
    if (pageSize === null) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "pageSize는 1 이상 50 이하 정수여야 합니다."
      );
    }

    const skip = (page - 1) * pageSize;

    const [totalCount, foundPosts] = await prisma.$transaction([
      prisma.posts.count({
        where: {
          status: "PUBLISHED"
        }
      }),
      prisma.posts.findMany({
        where: {
          status: "PUBLISHED"
        },
        orderBy: [
          {
            createdAt: "desc"
          },
          {
            id: "desc"
          }
        ],
        skip,
        take: pageSize,
        select: postBaseSelect
      })
    ]);

    const totalPages = totalCount === 0 ? 0 : Math.ceil(totalCount / pageSize);

    return res.status(200).json({
      posts: await Promise.all(foundPosts.map((post) => buildPostListItem(post))),
      page,
      pageSize,
      totalCount,
      totalPages,
      hasNext: page < totalPages
    });
  } catch (err) {
    console.error(err);
    return sendApiError(
      res,
      req,
      500,
      "INTERNAL_ERROR",
      "게시물 목록 조회 실패"
    );
  }
}

/**
 * 게시물 상세 조회
 * GET /api/v1/posts/:postId
 */
export async function getPostById(req, res) {
  try {
    const postId = parseBigInt(req.params?.postId);
    if (postId === null) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "postId는 숫자여야 합니다."
      );
    }

    const post = await prisma.posts.findFirst({
      where: {
        id: postId,
        status: "PUBLISHED"
      },
      select: postBaseSelect
    });

    if (!post) {
      return sendApiError(
        res,
        req,
        404,
        "POST_NOT_FOUND",
        "게시물을 찾을 수 없습니다."
      );
    }

    return res.status(200).json(await buildPostDetail(post));
  } catch (err) {
    console.error(err);
    return sendApiError(
      res,
      req,
      500,
      "INTERNAL_ERROR",
      "게시물 조회 실패"
    );
  }
}

/**
 * 공개 viewer 조회
 * GET /api/v1/posts/:postId/viewer
 */
export async function getPostViewer(req, res) {
  try {
    const sessionUserId = parseBigInt(req.user?.id);
    const postId = parseBigInt(req.params?.postId);
    if (postId === null) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "postId는 숫자여야 합니다."
      );
    }

    const post = await prisma.posts.findFirst({
      where: {
        id: postId,
        status: "PUBLISHED"
      },
      select: postBaseSelect
    });

    if (!post) {
      return sendApiError(
        res,
        req,
        404,
        "POST_NOT_FOUND",
        "게시물을 찾을 수 없습니다."
      );
    }

    const readModel = await buildJobReadModel(post.job, {
      bucketName: process.env.S3_BUCKET_NAME
    });
    const thumbnail = await buildPostThumbnailSummary(post);

    return res.status(200).json({
      id: toResponseId(post.id),
      postId: toResponseId(post.id),
      jobId: toResponseId(post.job.id),
      sceneId: toResponseId(post.job.sceneId),
      title: post.title,
      authorName: post.user.nickname ?? null,
      authorProfileImageUrl: post.user.profileImageUrl ?? null,
      isOwner: sessionUserId !== null && post.userId === sessionUserId,
      likeCount: Number(post.likeCount ?? 0),
      downloadCount: Number(post.downloadCount ?? 0),
      viewerReady: readModel.viewerReady,
      status: readModel.status,
      resultUrl: readModel.viewerReady ? readModel.resultUrl : null,
      file: readModel.viewerReady ? readModel.file : null,
      thumbnailUrl: thumbnail.thumbnailUrl,
      thumbnailUpdatedAt: thumbnail.thumbnailUpdatedAt,
      viewerPath: buildPostViewerApiPath(post.id)
    });
  } catch (err) {
    console.error(err);
    return sendApiError(
      res,
      req,
      500,
      "INTERNAL_ERROR",
      "공개 viewer 조회 실패"
    );
  }
}

/**
 * 게시물 썸네일 업로드용 Presigned URL 발급
 * POST /api/v1/posts/:postId/thumbnail/presign
 */
export async function issuePostThumbnailUploadPresign(req, res) {
  try {
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

    const postId = parseBigInt(req.params?.postId);
    if (postId === null) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "postId는 숫자여야 합니다."
      );
    }

    if (req.body?.contentType !== THUMBNAIL_CONTENT_TYPE) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "contentType은 image/jpeg만 허용됩니다."
      );
    }

    const loadedPost = await loadOwnedPublishedPost(postId, userId);
    if (loadedPost.error) {
      return sendApiError(
        res,
        req,
        loadedPost.error.status,
        loadedPost.error.code,
        loadedPost.error.message
      );
    }

    const post = loadedPost.post;
    const key = buildPostThumbnailKey(post.job.sceneId, post.job.id);
    const uploadUrl = await getSignedUrl(
      s3,
      new PutObjectCommand({
        Bucket: bucketName,
        Key: key,
        ContentType: THUMBNAIL_CONTENT_TYPE
      }),
      {
        expiresIn: THUMBNAIL_PRESIGNED_URL_EXPIRES_IN_SECONDS
      }
    );

    return res.status(200).json({
      ok: true,
      postId: toResponseId(post.id),
      sceneId: toResponseId(post.job.sceneId),
      jobId: toResponseId(post.job.id),
      key,
      uploadUrl,
      thumbnailUrl: (
        await buildThumbnailSummary({
          bucketName,
          post
        })
      ).thumbnailUrl,
      thumbnailUpdatedAt: post.thumbnailUpdatedAt?.toISOString() ?? null,
      expiresIn: THUMBNAIL_PRESIGNED_URL_EXPIRES_IN_SECONDS
    });
  } catch (err) {
    console.error(err);
    return sendApiError(
      res,
      req,
      500,
      "INTERNAL_ERROR",
      "게시물 썸네일 업로드 URL 발급 실패"
    );
  }
}

/**
 * 게시물 썸네일 업로드 완료 처리
 * POST /api/v1/posts/:postId/thumbnail/complete
 */
export async function completePostThumbnailUpload(req, res) {
  try {
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

    const postId = parseBigInt(req.params?.postId);
    if (postId === null) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "postId는 숫자여야 합니다."
      );
    }

    const key = normalizeStorageKey(req.body?.key);
    if (!key) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "key는 필수입니다."
      );
    }

    const loadedPost = await loadOwnedPublishedPost(postId, userId);
    if (loadedPost.error) {
      return sendApiError(
        res,
        req,
        loadedPost.error.status,
        loadedPost.error.code,
        loadedPost.error.message
      );
    }

    const post = loadedPost.post;
    const expectedKey = buildPostThumbnailKey(post.job.sceneId, post.job.id);
    if (key !== expectedKey) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "허용되지 않는 key입니다."
      );
    }

    let headResult;
    try {
      headResult = await s3.send(
        new HeadObjectCommand({
          Bucket: bucketName,
          Key: key
        })
      );
    } catch (err) {
      const statusCode = err?.$metadata?.httpStatusCode;
      const errorName = err?.name;

      if (statusCode === 404 || errorName === "NotFound" || errorName === "NoSuchKey") {
        return sendApiError(
          res,
          req,
          400,
          "BAD_REQUEST",
          "업로드된 썸네일을 찾을 수 없습니다."
        );
      }

      throw err;
    }

    const contentType =
      typeof headResult.ContentType === "string"
        ? headResult.ContentType.split(";")[0].trim().toLowerCase()
        : null;
    if (contentType !== THUMBNAIL_CONTENT_TYPE) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "image/jpeg 파일만 업로드할 수 있습니다."
      );
    }

    const contentLength = Number(headResult.ContentLength ?? 0);
    if (!Number.isFinite(contentLength) || contentLength <= 0) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "업로드된 썸네일 크기를 확인할 수 없습니다."
      );
    }

    if (contentLength > THUMBNAIL_MAX_BYTES) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "썸네일은 10MB 이하여야 합니다."
      );
    }

    const thumbnailUpdatedAt =
      headResult.LastModified instanceof Date &&
      !Number.isNaN(headResult.LastModified.getTime())
        ? headResult.LastModified
        : new Date();

    const updatedPost = await prisma.posts.update({
      where: {
        id: post.id
      },
      data: {
        thumbnailKey: key,
        thumbnailUpdatedAt
      },
      select: {
        id: true,
        thumbnailKey: true,
        thumbnailUpdatedAt: true
      }
    });

    const thumbnail = await buildThumbnailSummary({
      bucketName,
      post: updatedPost
    });

    return res.status(200).json({
      ok: true,
      postId: toResponseId(post.id),
      sceneId: toResponseId(post.job.sceneId),
      jobId: toResponseId(post.job.id),
      key: updatedPost.thumbnailKey,
      thumbnailUrl: thumbnail.thumbnailUrl,
      thumbnailUpdatedAt: thumbnail.thumbnailUpdatedAt
    });
  } catch (err) {
    console.error(err);
    return sendApiError(
      res,
      req,
      500,
      "INTERNAL_ERROR",
      "게시물 썸네일 업로드 완료 처리 실패"
    );
  }
}
