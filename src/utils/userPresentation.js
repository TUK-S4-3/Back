import { buildSignedGetObjectUrl } from "./jobPresentation.js";

function normalizeString(value) {
  if (typeof value !== "string" || value.trim().length === 0) {
    return null;
  }

  return value.trim();
}

function toIsoStringOrNull(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return null;
  }

  return date.toISOString();
}

function isHttpUrl(value) {
  return /^https?:\/\//i.test(value);
}

export async function buildUserProfileImageSummary(user, options = {}) {
  const bucketName = options.bucketName ?? process.env.S3_BUCKET_NAME ?? null;
  const profileImageKey = normalizeString(user?.profileImageKey);
  const rawProfileImageUrl = normalizeString(user?.profileImageUrl);
  const profileImageUpdatedAt = toIsoStringOrNull(user?.profileImageUpdatedAt);

  if (profileImageKey) {
    return {
      profileImageKey,
      profileImageUpdatedAt,
      profileImageUrl: await buildSignedGetObjectUrl(bucketName, profileImageKey)
    };
  }

  if (!rawProfileImageUrl) {
    return {
      profileImageKey: null,
      profileImageUpdatedAt: null,
      profileImageUrl: null
    };
  }

  if (isHttpUrl(rawProfileImageUrl)) {
    return {
      profileImageKey: null,
      profileImageUpdatedAt,
      profileImageUrl: rawProfileImageUrl
    };
  }

  return {
    profileImageKey: rawProfileImageUrl,
    profileImageUpdatedAt,
    profileImageUrl: await buildSignedGetObjectUrl(bucketName, rawProfileImageUrl)
  };
}
