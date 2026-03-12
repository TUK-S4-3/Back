import { HeadObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { prisma } from "../db.config.js";
import { sendApiError } from "../utils/apiError.js";
import { buildThumbnailSummary } from "../utils/jobPresentation.js";
import { s3 } from "../utils/s3.js";
import { buildUserProfileImageSummary } from "../utils/userPresentation.js";

const DEFAULT_PAGE = 1;
const DEFAULT_PAGE_SIZE = 20;
const MAX_PAGE_SIZE = 50;
const PROFILE_IMAGE_PRESIGNED_URL_EXPIRES_IN_SECONDS = 60 * 5;
const PROFILE_IMAGE_MAX_BYTES = 5 * 1024 * 1024;

const ALLOWED_PROFILE_IMAGE_CONTENT_TYPES = {
  "image/jpeg": "jpg",
  "image/png": "png",
  "image/webp": "webp"
};

const currentUserSelect = {
  id: true,
  provider: true,
  nickname: true,
  profileImageUrl: true,
  profileImageKey: true,
  profileImageUpdatedAt: true,
  createdAt: true,
  updatedAt: true
};

const myPostSelect = {
  id: true,
  title: true,
  likeCount: true,
  downloadCount: true,
  createdAt: true,
  updatedAt: true,
  thumbnailKey: true,
  thumbnailUpdatedAt: true,
  job: {
    select: {
      id: true,
      sceneId: true,
      thumbnailKey: true,
      updatedAt: true,
      scene: {
        select: {
          id: true,
          thumbnailKey: true,
          updatedAt: true
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

function normalizeNickname(value) {
  if (typeof value !== "string") {
    return null;
  }

  const normalized = value.trim();
  if (normalized.length === 0 || normalized.length > 50) {
    return null;
  }

  return normalized;
}

function normalizeStorageKey(value) {
  if (typeof value !== "string" || value.trim().length === 0) {
    return null;
  }

  return value.trim();
}

function parseProfileImageContentType(value) {
  if (typeof value !== "string") {
    return null;
  }

  const normalized = value.split(";")[0].trim().toLowerCase();
  if (!Object.hasOwn(ALLOWED_PROFILE_IMAGE_CONTENT_TYPES, normalized)) {
    return null;
  }

  return normalized;
}

function buildProfileImageKey(userId, contentType) {
  const extension = ALLOWED_PROFILE_IMAGE_CONTENT_TYPES[contentType];
  return `users/${userId}/profile/profile.${extension}`;
}

function buildProfileImagePrefix(userId) {
  return `users/${userId}/profile/profile.`;
}

function buildPostViewerApiPath(postId) {
  return `/api/v1/posts/${toResponseId(postId)}/viewer`;
}

async function loadCurrentUser(userId) {
  return prisma.users.findUnique({
    where: {
      id: userId
    },
    select: currentUserSelect
  });
}

async function buildCurrentUserResponse(user) {
  const profileImage = await buildUserProfileImageSummary(user, {
    bucketName: process.env.S3_BUCKET_NAME
  });

  return {
    id: toResponseId(user.id),
    email: null,
    nickname: user.nickname ?? null,
    provider: String(user.provider).toLowerCase(),
    profileImageUrl: profileImage.profileImageUrl,
    profileImageUpdatedAt: profileImage.profileImageUpdatedAt,
    createdAt: user.createdAt.toISOString()
  };
}

async function buildMyPostListItem(post) {
  const thumbnail = await buildThumbnailSummary({
    bucketName: process.env.S3_BUCKET_NAME,
    post,
    job: post.job,
    scene: post.job.scene
  });

  return {
    postId: toResponseId(post.id),
    title: post.title,
    thumbnailUrl: thumbnail.thumbnailUrl,
    createdAt: post.createdAt.toISOString(),
    likeCount: Number(post.likeCount ?? 0),
    downloadCount: Number(post.downloadCount ?? 0),
    sceneId: toResponseId(post.job.sceneId),
    jobId: toResponseId(post.job.id),
    viewerPath: buildPostViewerApiPath(post.id)
  };
}

export async function getMyProfile(req, res) {
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

    const user = await loadCurrentUser(userId);
    if (!user) {
      return sendApiError(
        res,
        req,
        404,
        "USER_NOT_FOUND",
        "사용자를 찾을 수 없습니다."
      );
    }

    return res.status(200).json(await buildCurrentUserResponse(user));
  } catch (err) {
    console.error(err);
    return sendApiError(
      res,
      req,
      500,
      "INTERNAL_ERROR",
      "프로필 조회 실패"
    );
  }
}

export async function updateMyProfile(req, res) {
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

    const nickname = normalizeNickname(req.body?.nickname);
    if (nickname === null) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "nickname은 1자 이상 50자 이하 문자열이어야 합니다."
      );
    }

    const updated = await prisma.users.update({
      where: {
        id: userId
      },
      data: {
        nickname
      },
      select: {
        id: true,
        nickname: true
      }
    });

    return res.status(200).json({
      ok: true,
      message: "닉네임이 변경되었습니다.",
      user: {
        id: toResponseId(updated.id),
        nickname: updated.nickname ?? null
      }
    });
  } catch (err) {
    if (err?.code === "P2002") {
      return sendApiError(
        res,
        req,
        409,
        "NICKNAME_ALREADY_EXISTS",
        "이미 사용 중인 닉네임입니다."
      );
    }

    console.error(err);
    return sendApiError(
      res,
      req,
      500,
      "INTERNAL_ERROR",
      "닉네임 변경 실패"
    );
  }
}

export async function issueProfileImageUploadPresign(req, res) {
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

    const contentType = parseProfileImageContentType(req.body?.contentType);
    if (contentType === null) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "contentType은 image/jpeg, image/png, image/webp만 허용됩니다."
      );
    }

    const key = buildProfileImageKey(userId.toString(), contentType);
    const uploadUrl = await getSignedUrl(
      s3,
      new PutObjectCommand({
        Bucket: bucketName,
        Key: key,
        ContentType: contentType
      }),
      {
        expiresIn: PROFILE_IMAGE_PRESIGNED_URL_EXPIRES_IN_SECONDS
      }
    );

    const profileImageUrl = await buildUserProfileImageSummary(
      {
        profileImageKey: key,
        profileImageUpdatedAt: new Date()
      },
      {
        bucketName
      }
    );

    return res.status(200).json({
      ok: true,
      uploadUrl,
      key,
      profileImageUrl: profileImageUrl.profileImageUrl,
      profileImageUpdatedAt: null,
      expiresIn: PROFILE_IMAGE_PRESIGNED_URL_EXPIRES_IN_SECONDS
    });
  } catch (err) {
    console.error(err);
    return sendApiError(
      res,
      req,
      500,
      "INTERNAL_ERROR",
      "프로필 이미지 업로드 URL 발급 실패"
    );
  }
}

export async function completeProfileImageUpload(req, res) {
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

    const expectedPrefix = buildProfileImagePrefix(userId.toString());
    if (!key.startsWith(expectedPrefix)) {
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
          "업로드된 프로필 이미지를 찾을 수 없습니다."
        );
      }

      throw err;
    }

    const contentType = parseProfileImageContentType(headResult.ContentType);
    if (contentType === null) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "허용되지 않는 프로필 이미지 형식입니다."
      );
    }

    const contentLength = Number(headResult.ContentLength ?? 0);
    if (!Number.isFinite(contentLength) || contentLength <= 0) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "업로드된 프로필 이미지 크기를 확인할 수 없습니다."
      );
    }

    if (contentLength > PROFILE_IMAGE_MAX_BYTES) {
      return sendApiError(
        res,
        req,
        400,
        "BAD_REQUEST",
        "프로필 이미지는 5MB 이하여야 합니다."
      );
    }

    const profileImageUpdatedAt =
      headResult.LastModified instanceof Date &&
      !Number.isNaN(headResult.LastModified.getTime())
        ? headResult.LastModified
        : new Date();

    const updated = await prisma.users.update({
      where: {
        id: userId
      },
      data: {
        profileImageKey: key,
        profileImageUpdatedAt
      },
      select: currentUserSelect
    });

    const profileImage = await buildUserProfileImageSummary(updated, {
      bucketName
    });

    return res.status(200).json({
      ok: true,
      profileImageUrl: profileImage.profileImageUrl,
      profileImageUpdatedAt: profileImage.profileImageUpdatedAt
    });
  } catch (err) {
    console.error(err);
    return sendApiError(
      res,
      req,
      500,
      "INTERNAL_ERROR",
      "프로필 이미지 업로드 완료 처리 실패"
    );
  }
}

export async function listMyPosts(req, res) {
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
    const [totalCount, posts] = await prisma.$transaction([
      prisma.posts.count({
        where: {
          userId,
          status: "PUBLISHED"
        }
      }),
      prisma.posts.findMany({
        where: {
          userId,
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
        select: myPostSelect
      })
    ]);

    const totalPages = totalCount === 0 ? 0 : Math.ceil(totalCount / pageSize);

    return res.status(200).json({
      items: await Promise.all(posts.map((post) => buildMyPostListItem(post))),
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
      "내 게시물 목록 조회 실패"
    );
  }
}
