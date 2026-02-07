import { PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { s3 } from "../utils/s3.js";
import { v4 as uuid } from "uuid";
import { getUploads, saveUploads } from "../services/uploadService.js";

/**
 * 사용자 파일 업로드
 * POST /api/uploads
 */
export async function uploadFile(req, res) {
  try {
    const user = req.user;
    const file = req.file;

    if (!file) {
      return res.status(400).json({
        ok: false,
        message: "파일이 없습니다."
      });
    }

    const fileKey = `uploads/original/${user.id}_${uuid()}_${file.originalname}`;

    await s3.send(
      new PutObjectCommand({
        Bucket: process.env.AWS_S3_BUCKET,
        Key: fileKey,
        Body: file.buffer,
        ContentType: file.mimetype
      })
    );

    const uploads = await getUploads();

    const newUpload = {
      id: Date.now(),
      userId: user.id,
      originalFileKey: fileKey,
      status: "UPLOADED",
      createdAt: new Date().toISOString()
    };

    uploads.push(newUpload);
    await saveUploads(uploads);

    return res.status(201).json({
      ok: true,
      upload: newUpload
    });

  } catch (err) {
    console.error(err);
    return res.status(500).json({
      ok: false,
      message: "파일 업로드 실패"
    });
  }
}

/**
 * 내 업로드 목록 조회
 * GET /api/uploads/my
 */
export async function getMyUploads(req, res) {
  try {
    const user = req.user;
    const uploads = await getUploads();

    const myUploads = uploads
      .filter(u => u.userId === user.id)
      .map(u => ({
        id: u.id,
        status: u.status,
        createdAt: u.createdAt,
        completedAt: u.completedAt || null
      }));

    return res.json({
      ok: true,
      uploads: myUploads
    });

  } catch (err) {
    console.error(err);
    return res.status(500).json({
      ok: false,
      message: "업로드 목록 조회 실패"
    });
  }
}

/**
 * 3D 결과 파일 다운로드 (Presigned URL)
 * GET /api/uploads/:id/result
 */
export async function downloadResultFile(req, res) {
  try {
    const user = req.user;
    const uploadId = Number(req.params.id);

    const uploads = await getUploads();
    const target = uploads.find(u => u.id === uploadId);

    if (!target) {
      return res.status(404).json({
        ok: false,
        message: "업로드 정보를 찾을 수 없습니다."
      });
    }

    // 🔐 본인 파일만 허용
    if (target.userId !== user.id) {
      return res.status(403).json({
        ok: false,
        message: "접근 권한이 없습니다."
      });
    }

    if (target.status !== "DONE" || !target.resultFileKey) {
      return res.status(400).json({
        ok: false,
        message: "아직 결과 파일이 준비되지 않았습니다."
      });
    }

    const command = new GetObjectCommand({
      Bucket: process.env.AWS_S3_BUCKET,
      Key: target.resultFileKey
    });

    const url = await getSignedUrl(s3, command, {
      expiresIn: 60 * 5 // 5분
    });

    return res.json({
      ok: true,
      url
    });

  } catch (err) {
    console.error(err);
    return res.status(500).json({
      ok: false,
      message: "결과 파일 다운로드 실패"
    });
  }
}
