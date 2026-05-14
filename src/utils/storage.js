import { GetObjectCommand, HeadObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import fs from "node:fs/promises";
import path from "node:path";
import { s3 } from "./s3.js";
import { streamToString } from "./stream.js";

const DEFAULT_STORAGE_DRIVER = "local";
const DEFAULT_LOCAL_ASSET_BASE_URL = "/local-assets";

const CONTENT_TYPES_BY_EXTENSION = new Map([
  [".jpg", "image/jpeg"],
  [".jpeg", "image/jpeg"],
  [".png", "image/png"],
  [".webp", "image/webp"],
  [".mp4", "video/mp4"],
  [".json", "application/json"],
  [".ply", "application/octet-stream"]
]);

export function getStorageDriver() {
  const driver = String(process.env.STORAGE_DRIVER ?? DEFAULT_STORAGE_DRIVER)
    .trim()
    .toLowerCase();

  return driver === "s3" ? "s3" : "local";
}

export function isLocalStorage() {
  return getStorageDriver() === "local";
}

export function normalizeStorageKey(value) {
  if (typeof value !== "string" || value.trim().length === 0) {
    return null;
  }

  const normalized = value.trim().replaceAll("\\", "/");
  if (
    normalized.startsWith("/") ||
    normalized.includes("\0") ||
    normalized.split("/").some((segment) => segment === "" || segment === "." || segment === "..")
  ) {
    return null;
  }

  return normalized;
}

export function getLocalStorageRoot() {
  return path.resolve(process.env.LOCAL_STORAGE_ROOT ?? path.join(process.cwd(), "..", "data", "storage"));
}

export function resolveLocalStoragePath(key) {
  const normalizedKey = normalizeStorageKey(key);
  if (!normalizedKey) {
    return null;
  }

  const root = getLocalStorageRoot();
  const filePath = path.resolve(root, normalizedKey);
  if (filePath !== root && !filePath.startsWith(`${root}${path.sep}`)) {
    return null;
  }

  return {
    root,
    key: normalizedKey,
    filePath
  };
}

function encodeStorageKey(key) {
  return key
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
}

function inferContentType(key) {
  return CONTENT_TYPES_BY_EXTENSION.get(path.extname(key).toLowerCase()) ?? "application/octet-stream";
}

export function buildPublicStorageUrl(bucketName, key) {
  const normalizedKey = normalizeStorageKey(key);
  if (!normalizedKey) {
    return null;
  }

  const encodedKey = encodeStorageKey(normalizedKey);
  if (isLocalStorage()) {
    const baseUrl = String(process.env.LOCAL_ASSET_BASE_URL ?? DEFAULT_LOCAL_ASSET_BASE_URL).replace(/\/+$/, "");
    return `${baseUrl}/${encodedKey}`;
  }

  if (!bucketName) {
    return null;
  }

  const region = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION;
  if (region) {
    return `https://${bucketName}.s3.${region}.amazonaws.com/${encodedKey}`;
  }

  return `https://${bucketName}.s3.amazonaws.com/${encodedKey}`;
}

export async function buildStorageUploadUrl({
  bucketName,
  key,
  contentType,
  expiresIn,
  localUrl
}) {
  const normalizedKey = normalizeStorageKey(key);
  if (!normalizedKey) {
    return null;
  }

  if (isLocalStorage()) {
    return localUrl;
  }

  if (!bucketName) {
    return null;
  }

  return getSignedUrl(
    s3,
    new PutObjectCommand({
      Bucket: bucketName,
      Key: normalizedKey,
      ContentType: contentType
    }),
    { expiresIn }
  );
}

export async function buildStorageGetUrl(bucketName, key, { expiresIn } = {}) {
  const normalizedKey = normalizeStorageKey(key);
  if (!normalizedKey) {
    return null;
  }

  if (isLocalStorage()) {
    return buildPublicStorageUrl(bucketName, normalizedKey);
  }

  if (!bucketName) {
    return null;
  }

  return getSignedUrl(
    s3,
    new GetObjectCommand({
      Bucket: bucketName,
      Key: normalizedKey
    }),
    { expiresIn }
  );
}

export async function writeLocalStorageObject(key, body) {
  if (!isLocalStorage()) {
    throw new Error("local storage driver is not enabled");
  }

  const resolved = resolveLocalStoragePath(key);
  if (!resolved) {
    throw new Error("invalid storage key");
  }

  await fs.mkdir(path.dirname(resolved.filePath), { recursive: true });
  await fs.writeFile(resolved.filePath, body);
  return resolved;
}

export async function headStorageObject(bucketName, key) {
  const normalizedKey = normalizeStorageKey(key);
  if (!normalizedKey) {
    return null;
  }

  if (isLocalStorage()) {
    const resolved = resolveLocalStoragePath(normalizedKey);
    if (!resolved) {
      return null;
    }

    try {
      const stat = await fs.stat(resolved.filePath);
      if (!stat.isFile()) {
        return null;
      }

      return {
        ContentLength: stat.size,
        ContentType: inferContentType(normalizedKey),
        LastModified: stat.mtime,
        ETag: `"${stat.size.toString(16)}-${Math.round(stat.mtimeMs).toString(16)}"`,
        AcceptRanges: "bytes"
      };
    } catch (err) {
      if (err?.code === "ENOENT") {
        return null;
      }
      throw err;
    }
  }

  if (!bucketName) {
    return null;
  }

  try {
    return await s3.send(
      new HeadObjectCommand({
        Bucket: bucketName,
        Key: normalizedKey
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

export async function readJsonStorageObject(bucketName, key) {
  const normalizedKey = normalizeStorageKey(key);
  if (!normalizedKey) {
    return null;
  }

  try {
    let raw;
    if (isLocalStorage()) {
      const resolved = resolveLocalStoragePath(normalizedKey);
      if (!resolved) {
        return null;
      }
      raw = await fs.readFile(resolved.filePath, "utf8");
    } else {
      if (!bucketName) {
        return null;
      }

      const result = await s3.send(
        new GetObjectCommand({
          Bucket: bucketName,
          Key: normalizedKey
        })
      );

      if (!result.Body) {
        return null;
      }

      raw = await streamToString(result.Body);
    }

    if (typeof raw !== "string" || raw.trim().length === 0) {
      return null;
    }

    const parsed = JSON.parse(raw);
    return parsed !== null && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : null;
  } catch (err) {
    const statusCode = err?.$metadata?.httpStatusCode;
    const errorName = err?.name;
    if (
      err?.code === "ENOENT" ||
      statusCode === 404 ||
      errorName === "NotFound" ||
      errorName === "NoSuchKey"
    ) {
      return null;
    }
    throw err;
  }
}
