import { v4 as uuid } from "uuid";
import fs from "node:fs/promises";
import path from "node:path";
import { spawn } from "node:child_process";
import { prisma } from "../db.config.js";
import {
  buildStorageUploadUrl,
  getStorageDriver,
  headStorageObject,
  isLocalStorage,
  normalizeStorageKey,
  resolveLocalStoragePath,
  writeLocalStorageObject
} from "../utils/storage.js";

const ALLOWED_VIDEO_CONTENT_TYPES = ["video/mp4"];
const PRESIGNED_URL_EXPIRES_IN_SECONDS = 60 * 5;
const DEFAULT_UPLOADING_SCENE_TTL_MINUTES = 60;
const SCENE_LIST_PAGE_SIZE = 5;
const DEFAULT_VIDEO_TRANSCODE_CRF = "20";
const DEFAULT_VIDEO_TRANSCODE_PRESET = "veryfast";
const DEFAULT_VIDEO_AUDIO_BITRATE = "128k";
const PLAYBACK_VIDEO_SUFFIX = "_h264.mp4";

function parseBigInt(value) {
  try {
    return BigInt(value);
  } catch {
    return null;
  }
}

function parsePositiveInt(value) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(parsed) || parsed < 1) {
    return null;
  }
  return parsed;
}

function getUploadingSceneTtlMinutes() {
  const raw = Number(process.env.UPLOADING_SCENE_TTL_MINUTES);
  if (Number.isFinite(raw) && raw > 0) {
    return raw;
  }
  return DEFAULT_UPLOADING_SCENE_TTL_MINUTES;
}

function buildExpectedVideoInputPrefix(sceneId) {
  return `scenes/${sceneId}/input/video/`;
}

function isVideoTranscodeEnabled() {
  const raw = String(process.env.VIDEO_TRANSCODE_ENABLED ?? "true").trim().toLowerCase();
  return raw !== "false" && raw !== "0" && raw !== "off";
}

function buildBrowserPlaybackVideoKey(inputKey) {
  const parsed = path.posix.parse(inputKey);
  if (parsed.base.endsWith(PLAYBACK_VIDEO_SUFFIX)) {
    return inputKey;
  }
  return `${parsed.dir}/${parsed.name}${PLAYBACK_VIDEO_SUFFIX}`;
}

function appendLimitedStderr(current, chunk) {
  const next = `${current}${chunk}`;
  return next.length > 4000 ? next.slice(next.length - 4000) : next;
}

function runFfmpegTranscode(inputPath, outputPath) {
  const ffmpegPath = String(process.env.FFMPEG_PATH ?? "ffmpeg").trim() || "ffmpeg";
  const args = [
    "-y",
    "-i",
    inputPath,
    "-map",
    "0:v:0",
    "-map",
    "0:a?",
    "-c:v",
    "libx264",
    "-preset",
    String(process.env.VIDEO_TRANSCODE_PRESET ?? DEFAULT_VIDEO_TRANSCODE_PRESET),
    "-crf",
    String(process.env.VIDEO_TRANSCODE_CRF ?? DEFAULT_VIDEO_TRANSCODE_CRF),
    "-pix_fmt",
    "yuv420p",
    "-profile:v",
    "high",
    "-level",
    "4.1",
    "-c:a",
    "aac",
    "-b:a",
    String(process.env.VIDEO_TRANSCODE_AUDIO_BITRATE ?? DEFAULT_VIDEO_AUDIO_BITRATE),
    "-movflags",
    "+faststart",
    outputPath
  ];

  return new Promise((resolve, reject) => {
    const child = spawn(ffmpegPath, args, {
      stdio: ["ignore", "ignore", "pipe"]
    });
    let stderr = "";

    child.stderr.on("data", (chunk) => {
      stderr = appendLimitedStderr(stderr, chunk.toString("utf8"));
    });
    child.on("error", (err) => {
      reject(err);
    });
    child.on("close", (exitCode) => {
      if (exitCode === 0) {
        resolve();
        return;
      }
      reject(new Error(`ffmpeg exited with code ${exitCode}: ${stderr.trim()}`));
    });
  });
}

async function prepareBrowserCompatibleLocalVideo(inputKey) {
  if (!isLocalStorage() || !isVideoTranscodeEnabled()) {
    return inputKey;
  }

  const input = resolveLocalStoragePath(inputKey);
  const outputKey = buildBrowserPlaybackVideoKey(inputKey);
  const output = resolveLocalStoragePath(outputKey);
  if (!input || !output) {
    throw new Error("invalid local video storage key");
  }

  if (input.filePath === output.filePath) {
    return inputKey;
  }

  await fs.mkdir(path.dirname(output.filePath), { recursive: true });
  await runFfmpegTranscode(input.filePath, output.filePath);
  return outputKey;
}

function buildLocalVideoUploadUrl(sceneId, key) {
  const params = new URLSearchParams({
    sceneId: sceneId.toString(),
    key
  });
  return `/api/videos/local-upload?${params.toString()}`;
}

function isAllowedVideoContentType(value) {
  const contentType = String(value ?? "").split(";")[0].trim().toLowerCase();
  return ALLOWED_VIDEO_CONTENT_TYPES.includes(contentType) ? contentType : null;
}

async function cleanupExpiredUploadingScenes() {
  const ttlMinutes = getUploadingSceneTtlMinutes();
  const cutoff = new Date(Date.now() - ttlMinutes * 60 * 1000);

  return prisma.scenes.deleteMany({
    where: {
      status: "UPLOADING",
      inputVideoKey: null,
      createdAt: {
        lt: cutoff
      }
    }
  });
}

/**
 * 영상 업로드용 Presigned URL 발급
 * POST /api/videos/presign
 */
export async function issueVideoUploadPresign(req, res) {
  try {
    const bucketName = process.env.S3_BUCKET_NAME;
    if (getStorageDriver() === "s3" && !bucketName) {
      return res.status(500).json({
        ok: false,
        message: "S3 버킷 설정이 없습니다."
      });
    }

    const userId = parseBigInt(req.user?.id);
    if (userId === null) {
      return res.status(401).json({
        ok: false,
        message: "세션 사용자 정보가 유효하지 않습니다."
      });
    }

    const { filename, contentType, title } = req.body ?? {};

    if (typeof filename !== "string" || filename.trim().length === 0) {
      return res.status(400).json({
        ok: false,
        message: "filename은 필수입니다."
      });
    }

    if (typeof contentType !== "string" || contentType.trim().length === 0) {
      return res.status(400).json({
        ok: false,
        message: "contentType은 필수입니다."
      });
    }

    if (!ALLOWED_VIDEO_CONTENT_TYPES.includes(contentType)) {
      return res.status(400).json({
        ok: false,
        message: "지원하지 않는 영상 형식입니다."
      });
    }

    let normalizedTitle = null;
    if (title !== undefined && title !== null) {
      if (typeof title !== "string" || title.trim().length === 0) {
        return res.status(400).json({
          ok: false,
          message: "title은 비어있을 수 없습니다."
        });
      }

      normalizedTitle = title.trim();
      if (normalizedTitle.length > 100) {
        return res.status(400).json({
          ok: false,
          message: "title은 100자 이하여야 합니다."
        });
      }
    }

    const uploadId = uuid();
    const scene = await prisma.scenes.create({
      data: {
        userId,
        status: "UPLOADING",
        uploadId,
        ...(normalizedTitle ? { title: normalizedTitle } : {})
      }
    });

    const key = `scenes/${scene.id.toString()}/input/video/${uploadId}.mp4`;

    const url = await buildStorageUploadUrl({
      bucketName,
      key,
      contentType,
      expiresIn: PRESIGNED_URL_EXPIRES_IN_SECONDS,
      localUrl: buildLocalVideoUploadUrl(scene.id, key)
    });

    return res.status(201).json({
      ok: true,
      sceneId: scene.id.toString(),
      uploadId,
      key,
      url,
      expiresIn: PRESIGNED_URL_EXPIRES_IN_SECONDS
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({
      ok: false,
      message: "영상 업로드 URL 발급 실패"
    });
  }
}

export async function uploadVideoToLocalStorage(req, res) {
  try {
    if (!isLocalStorage()) {
      return res.status(404).json({
        ok: false,
        message: "로컬 저장소 모드가 아닙니다."
      });
    }

    const userId = parseBigInt(req.user?.id);
    if (userId === null) {
      return res.status(401).json({
        ok: false,
        message: "세션 사용자 정보가 유효하지 않습니다."
      });
    }

    const parsedSceneId = parseBigInt(req.query?.sceneId);
    if (parsedSceneId === null) {
      return res.status(400).json({
        ok: false,
        message: "sceneId는 필수입니다."
      });
    }

    const normalizedKey = normalizeStorageKey(req.query?.key);
    if (!normalizedKey) {
      return res.status(400).json({
        ok: false,
        message: "key는 필수입니다."
      });
    }

    const contentType = isAllowedVideoContentType(req.headers["content-type"]);
    if (!contentType) {
      return res.status(400).json({
        ok: false,
        message: "지원하지 않는 영상 형식입니다."
      });
    }

    if (!Buffer.isBuffer(req.body) || req.body.length === 0) {
      return res.status(400).json({
        ok: false,
        message: "업로드된 영상을 찾을 수 없습니다."
      });
    }

    const scene = await prisma.scenes.findUnique({
      where: {
        id: parsedSceneId
      }
    });

    if (!scene) {
      return res.status(404).json({
        ok: false,
        message: "scene을 찾을 수 없습니다."
      });
    }

    if (scene.userId !== userId) {
      return res.status(403).json({
        ok: false,
        message: "접근 권한이 없습니다."
      });
    }

    const expectedPrefix = buildExpectedVideoInputPrefix(parsedSceneId.toString());
    const expectedKey = `${expectedPrefix}${scene.uploadId}.mp4`;
    if (normalizedKey !== expectedKey) {
      return res.status(400).json({
        ok: false,
        message: "scene의 uploadId와 일치하지 않는 key입니다."
      });
    }

    await writeLocalStorageObject(normalizedKey, req.body);

    return res.status(200).json({
      ok: true,
      key: normalizedKey,
      size: req.body.length
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({
      ok: false,
      message: "로컬 영상 업로드 실패"
    });
  }
}

/**
 * 영상 업로드 완료 신고 + S3 검증
 * POST /api/videos/complete
 */
export async function completeVideoUpload(req, res) {
  try {
    const bucketName = process.env.S3_BUCKET_NAME;
    if (getStorageDriver() === "s3" && !bucketName) {
      return res.status(500).json({
        ok: false,
        message: "S3 버킷 설정이 없습니다."
      });
    }

    const userId = parseBigInt(req.user?.id);
    if (userId === null) {
      return res.status(401).json({
        ok: false,
        message: "세션 사용자 정보가 유효하지 않습니다."
      });
    }

    try {
      await cleanupExpiredUploadingScenes();
    } catch (cleanupErr) {
      console.error("cleanupExpiredUploadingScenes failed", cleanupErr);
    }

    const { sceneId, key } = req.body ?? {};
    const parsedSceneId = parseBigInt(sceneId);

    if (parsedSceneId === null) {
      return res.status(400).json({
        ok: false,
        message: "sceneId는 필수입니다."
      });
    }

    if (typeof key !== "string" || key.trim().length === 0) {
      return res.status(400).json({
        ok: false,
        message: "key는 필수입니다."
      });
    }

    const normalizedKey = normalizeStorageKey(key);
    if (!normalizedKey) {
      return res.status(400).json({
        ok: false,
        message: "key는 필수입니다."
      });
    }

    const scene = await prisma.scenes.findUnique({
      where: {
        id: parsedSceneId
      }
    });

    if (!scene) {
      return res.status(404).json({
        ok: false,
        message: "scene을 찾을 수 없습니다."
      });
    }

    if (scene.userId !== userId) {
      return res.status(403).json({
        ok: false,
        message: "접근 권한이 없습니다."
      });
    }

    const expectedPrefix = buildExpectedVideoInputPrefix(parsedSceneId.toString());

    if (!normalizedKey.startsWith(expectedPrefix)) {
      return res.status(400).json({
        ok: false,
        message: "허용되지 않는 key입니다."
      });
    }

    const sceneUploadId =
      typeof scene.uploadId === "string" && scene.uploadId.trim().length > 0
        ? scene.uploadId.trim()
        : null;

    if (sceneUploadId) {
      const expectedKey = `${expectedPrefix}${sceneUploadId}.mp4`;
      if (normalizedKey !== expectedKey) {
        return res.status(400).json({
          ok: false,
          message: "scene의 uploadId와 일치하지 않는 key입니다."
        });
      }
    }

    const expectedPlaybackKey = buildBrowserPlaybackVideoKey(normalizedKey);
    if (scene.inputVideoKey === expectedPlaybackKey && expectedPlaybackKey !== normalizedKey) {
      return res.status(200).json({
        ok: true,
        sceneId: scene.id.toString(),
        inputVideoKey: scene.inputVideoKey
      });
    }

    if (
      scene.inputVideoKey &&
      scene.inputVideoKey !== normalizedKey &&
      scene.inputVideoKey !== expectedPlaybackKey
    ) {
      return res.status(409).json({
        ok: false,
        message: "이미 다른 영상 key가 저장되어 있습니다."
      });
    }

    const headResult = await headStorageObject(bucketName, normalizedKey);
    if (!headResult) {
      return res.status(400).json({
        ok: false,
        message: "업로드된 영상을 찾을 수 없습니다."
      });
    }

    let inputVideoKey = normalizedKey;
    try {
      inputVideoKey = await prepareBrowserCompatibleLocalVideo(normalizedKey);
    } catch (transcodeErr) {
      console.error("prepareBrowserCompatibleLocalVideo failed", transcodeErr);
      await prisma.scenes.update({
        where: {
          id: parsedSceneId
        },
        data: {
          status: "FAILED"
        }
      });
      return res.status(500).json({
        ok: false,
        message: "브라우저 호환 영상 변환 실패"
      });
    }

    if (scene.inputVideoKey === inputVideoKey) {
      return res.status(200).json({
        ok: true,
        sceneId: scene.id.toString(),
        inputVideoKey: scene.inputVideoKey
      });
    }

    const updated = await prisma.scenes.update({
      where: {
        id: parsedSceneId
      },
      data: {
        inputVideoKey,
        status: "UPLOADED"
      }
    });

    return res.status(200).json({
      ok: true,
      sceneId: updated.id.toString(),
      inputVideoKey: updated.inputVideoKey
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({
      ok: false,
      message: "영상 업로드 완료 처리 실패"
    });
  }
}

/**
 * 로그인 사용자 본인 scene 목록 조회
 * GET /api/videos/scenes?page=1
 */
export async function listMyScenes(req, res) {
  try {
    const userId = parseBigInt(req.user?.id);
    if (userId === null) {
      return res.status(401).json({
        ok: false,
        message: "세션 사용자 정보가 유효하지 않습니다."
      });
    }

    const pageParam = req.query?.page;
    const page = pageParam === undefined ? 1 : parsePositiveInt(pageParam);
    if (page === null) {
      return res.status(400).json({
        ok: false,
        message: "page는 1 이상의 정수여야 합니다."
      });
    }

    const skip = (page - 1) * SCENE_LIST_PAGE_SIZE;

    const [totalCount, scenes] = await prisma.$transaction([
      prisma.scenes.count({
        where: {
          userId
        }
      }),
      prisma.scenes.findMany({
        where: {
          userId
        },
        orderBy: {
          createdAt: "desc"
        },
        skip,
        take: SCENE_LIST_PAGE_SIZE,
        select: {
          id: true,
          title: true,
          uploadId: true,
          inputVideoKey: true,
          createdAt: true,
          updatedAt: true
        }
      })
    ]);

    const totalPages =
      totalCount === 0 ? 0 : Math.ceil(totalCount / SCENE_LIST_PAGE_SIZE);

    return res.status(200).json({
      ok: true,
      page,
      pageSize: SCENE_LIST_PAGE_SIZE,
      totalCount,
      totalPages,
      hasNext: page < totalPages,
      items: scenes.map((scene) => ({
        ...scene,
        id: scene.id.toString()
      }))
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({
      ok: false,
      message: "scene 목록 조회 실패"
    });
  }
}
