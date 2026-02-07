import multer from "multer";

export const upload = multer({
  storage: multer.memoryStorage(), // S3로 바로 올릴 거라 메모리
  limits: {
    fileSize: 1024 * 1024 * 100 // 100MB (조절 가능)
  },
  fileFilter(req, file, cb) {
    const allowed = [
      "image/jpeg",
      "image/png",
      "video/mp4"
    ];

    if (!allowed.includes(file.mimetype)) {
      cb(new Error("지원하지 않는 파일 형식입니다."));
    } else {
      cb(null, true);
    }
  }
});
