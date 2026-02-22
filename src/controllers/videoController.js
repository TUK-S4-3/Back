import { PutObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { v4 as uuid } from "uuid";
import { prisma } from "../db.config.js";
import { s3 } from "../utils/s3.js";

const ALLOWED_VIDEO_CONTENT_TYPES = ["video/mp4"];
const PRESIGNED_URL_EXPIRES_IN_SECONDS = 60 * 5;

function parseBigInt(value) {
  try {
    return BigInt(value);
  } catch {
    return null;
  }
}

function sanitizeFilename(filename) {
  return filename.trim().replace(/[^a-zA-Z0-9._-]/g, "_");
}

/**
 * 영상 업로드용 Presigned URL 발급
 * POST /api/videos/presign
 */
export async function issueVideoUploadPresign(req, res) {
  try {
    const bucketName = process.env.S3_BUCKET_NAME;
    if (!bucketName) {
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

    const { filename, contentType } = req.body ?? {};

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

    const scene = await prisma.scenes.create({
      data: {
        userId,
        status: "UPLOADING"
      }
    });

    const safeFilename = sanitizeFilename(filename);
    const key = `videos/input/${userId.toString()}/${scene.id.toString()}/${uuid()}_${safeFilename}`;

    const command = new PutObjectCommand({
      Bucket: bucketName,
      Key: key,
      ContentType: contentType
    });

    const url = await getSignedUrl(s3, command, {
      expiresIn: PRESIGNED_URL_EXPIRES_IN_SECONDS
    });

    return res.status(201).json({
      ok: true,
      sceneId: scene.id.toString(),
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
