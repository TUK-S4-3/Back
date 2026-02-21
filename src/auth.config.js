import dotenv from "dotenv";
import { Strategy as GoogleStrategy } from "passport-google-oauth20";
import { prisma } from "./db.config.js";

dotenv.config();

export const googleStrategy = new GoogleStrategy(
  {
    clientID: process.env.PASSPORT_GOOGLE_CLIENT_ID,
    clientSecret: process.env.PASSPORT_GOOGLE_CLIENT_SECRET,
    callbackURL: "http://localhost:3000/api/oauth2/callback/google",
    scope: ["email", "profile"],
    state: true,
  },
  (accessToken, refreshToken, profile, cb) => {
    return googleVerify(profile)
      .then((user) => cb(null, user))
      .catch((err) => cb(err));
  }
);


const googleVerify = async (profile) => {
  const providerId = profile.id;
  if (!providerId) {
    throw new Error("google profile.id was not found");
  }

  const user = await prisma.users.findUnique({
    where: {
      provider_providerId: {
        provider: "GOOGLE",
        providerId,
      },
    },
  });
  if (user !== null) {
    return {
      id: user.id.toString(),
      provider: user.provider,
      providerId: user.providerId,
      nickname: user.nickname,
    };
  }

  const created = await prisma.users.create({
    data: {
      provider: "GOOGLE",
      providerId,
      nickname: `google_${providerId}`,
      profileImageUrl: profile.photos?.[0]?.value ?? null,
    },
  });

  return {
    id: created.id.toString(),
    provider: created.provider,
    providerId: created.providerId,
    nickname: created.nickname,
  };
};
