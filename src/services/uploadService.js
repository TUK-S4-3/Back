import { GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { s3 } from "../utils/s3.js";
import { streamToString } from "../utils/stream.js";

const BUCKET = process.env.AWS_S3_BUCKET;
const KEY = "data/uploads.json";

export async function getUploads() {
  try {
    const data = await s3.send(
      new GetObjectCommand({ Bucket: BUCKET, Key: KEY })
    );
    const body = await streamToString(data.Body);
    return JSON.parse(body);
  } catch (err) {
    if (err.name === "NoSuchKey") return [];
    throw err;
  }
}

export async function saveUploads(uploads) {
  await s3.send(
    new PutObjectCommand({
      Bucket: BUCKET,
      Key: KEY,
      Body: JSON.stringify(uploads, null, 2),
      ContentType: "application/json"
    })
  );
}
