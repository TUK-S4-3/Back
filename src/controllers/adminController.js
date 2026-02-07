import { PutObjectCommand } from "@aws-sdk/client-s3";
import { s3 } from "../utils/s3.js";
import { getUploads, saveUploads } from "../services/uploadService.js";
import { v4 as uuid } from "uuid";

/**
 * 관리자 - 전체 업로드 목록 조회
 * GET /api/admin/uploads
 */
export async function getAllUploads(req, res) {
  try {
    const uploads = await getUploads();

    return res.json({
      ok: true,
      uploads
    });

  } catch (err) {
    console.error(err);
    return res.status(500).json({
      ok: false,
      message: "전체 업로드 목록 조회 실패"
    });
  }
}

/**
 * 관리자 - 3D 결과 파일 업로드
 * POST /api/admin/uploads/:id/result
 */
export async function uploadResultFile(req, res) {
  try {
    const uploadId = Number(req.params.id);
    const file = req.file;

    if (!file) {
      return res.status(400).json({
        ok: false,
        message: "결과 파일이 없습니다."
      });
    }

    const uploads = await getUploads();
    const target = uploads.find(u => u.id === uploadId);

    if (!target) {
      return res.status(404).json({
        ok: false,
        message: "업로드 정보를 찾을 수 없습니다."
      });
    }

    const resultKey = `uploads/result/${target.userId}_${uuid()}_${file.originalname}`;

    // S3에 결과 파일 업로드
    await s3.send(
      new PutObjectCommand({
        Bucket: process.env.AWS_S3_BUCKET,
        Key: resultKey,
        Body: file.buffer,
        ContentType: file.mimetype
      })
    );

    // 업로드 상태 업데이트
    target.resultFileKey = resultKey;
    target.status = "DONE";
    target.completedAt = new Date().toISOString();

    await saveUploads(uploads);

    return res.json({
      ok: true,
      upload: target
    });

  } catch (err) {
    console.error(err);
    return res.status(500).json({
      ok: false,
      message: "결과 파일 업로드 실패"
    });
  }
}
