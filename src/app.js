import express from 'express';
import dotenv from "dotenv"
import cors from 'cors';
import oauthRouter from "./routes/oauthRouter.js";
import userRoutes from "./routes/userRoutes.js";
import videoRoutes from "./routes/videoRoutes.js";
import v1Routes from "./routes/v1Routes.js";
import { PrismaSessionStore } from "@quixo3/prisma-session-store";
import session from "express-session";
import passport from "passport";
import { googleStrategy } from "./auth.config.js";
import { prisma } from "./db.config.js";
import { traceIdMiddleware } from "./middlewares/traceIdMiddleware.js";
import { buildUserProfileImageSummary } from "./utils/userPresentation.js";

dotenv.config();

passport.use(googleStrategy);
passport.serializeUser((user, done) => done(null, user.id));
passport.deserializeUser(async (id, done) => {
  try {
    const parsedId = typeof id === "bigint" ? id : BigInt(id);
    const user = await prisma.users.findUnique({ where: { id: parsedId } });
    if (!user) {
      return done(null, false);
    }
    return done(null, {
      id: user.id.toString(),
      name: user.nickname ?? null,
      email: null,
      provider: String(user.provider).toLowerCase(),
    });
  } catch (err) {
    return done(err);
  }
});

const app = express();
const PORT = process.env.PORT;
const isProduction = process.env.NODE_ENV === "production";
const corsOrigins = (
  process.env.CORS_ORIGIN ?? process.env.FRONTEND_URL ?? "http://localhost:5173"
)
  .split(",")
  .map((origin) => origin.trim())
  .filter(Boolean);

const sessionCookieOptions = {
  maxAge: 7 * 24 * 60 * 60 * 1000, // ms
  httpOnly: true,
  secure: isProduction,
  sameSite: isProduction ? "none" : "lax",
};

const clearCookieOptions = {
  httpOnly: sessionCookieOptions.httpOnly,
  secure: sessionCookieOptions.secure,
  sameSite: sessionCookieOptions.sameSite,
  path: "/",
};

app.use(
  cors({
    origin: corsOrigins,
    credentials: true,
  })
);
app.use(express.static('public')); 
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(traceIdMiddleware);
app.use(
  session({
    cookie: sessionCookieOptions,
    resave: false,
    saveUninitialized: false,
    secret: process.env.EXPRESS_SESSION_SECRET,
    store: new PrismaSessionStore(prisma, {
      checkPeriod: 2 * 60 * 1000, // ms
      dbRecordIdIsSessionId: true,
      dbRecordIdFunction: undefined,
    }),
  })
);

app.use(passport.initialize());
app.use(passport.session());

app.get("/", (req, res) => {
  // #swagger.ignore = true
  console.log(req.user);
  res.send("Hello World!");
});

function parseBigInt(value) {
  try {
    return BigInt(value);
  } catch {
    return null;
  }
}

function toResponseId(value) {
  const numeric = Number(value);
  if (Number.isSafeInteger(numeric)) {
    return numeric;
  }

  return value.toString();
}

async function buildAuthResponseUserById(userId) {
  const user = await prisma.users.findUnique({
    where: {
      id: userId
    },
    select: {
      id: true,
      provider: true,
      nickname: true,
      profileImageUrl: true,
      profileImageKey: true,
      profileImageUpdatedAt: true
    }
  });

  if (!user) {
    return null;
  }

  const profileImage = await buildUserProfileImageSummary(user, {
    bucketName: process.env.S3_BUCKET_NAME
  });

  return {
    id: toResponseId(user.id),
    name: user.nickname ?? null,
    nickname: user.nickname ?? null,
    email: null,
    provider: String(user.provider).toLowerCase(),
    profileImageUrl: profileImage.profileImageUrl,
    profileImageUpdatedAt: profileImage.profileImageUpdatedAt
  };
}

async function getAuthSessionStatus(req, res) {
  const sessionUserId = parseBigInt(req.user?.id);
  if (!req.isAuthenticated?.() || sessionUserId === null) {
    return res.status(200).json({
      authenticated: false,
      user: null,
    });
  }

  const user = await buildAuthResponseUserById(sessionUserId);
  const authenticated = Boolean(user?.id);

  return res.status(200).json({
    authenticated,
    user: authenticated ? user : null,
  });
}

app.get("/api/auth/me", getAuthSessionStatus);
app.get("/api/auth/session", getAuthSessionStatus);

app.post("/api/auth/dev/login", async (req, res) => {
  if (process.env.NODE_ENV === "production") {
    return res.status(403).json({
      ok: false,
      message: "운영 환경에서는 사용할 수 없습니다.",
    });
  }

  try {
    const { userId, providerId, nickname } = req.body ?? {};
    let user = null;

    if (userId !== undefined && userId !== null && String(userId).trim() !== "") {
      try {
        user = await prisma.users.findUnique({
          where: {
            id: BigInt(userId),
          },
        });
      } catch {
        return res.status(400).json({
          ok: false,
          message: "userId 형식이 올바르지 않습니다.",
        });
      }
    }

    if (!user && typeof providerId === "string" && providerId.trim().length > 0) {
      user = await prisma.users.findUnique({
        where: {
          provider_providerId: {
            provider: "GOOGLE",
            providerId: providerId.trim(),
          },
        },
      });
    }

    if (!user) {
      const resolvedProviderId =
        typeof providerId === "string" && providerId.trim().length > 0
          ? providerId.trim()
          : `dev-${Date.now()}`;

      const resolvedNickname =
        typeof nickname === "string" && nickname.trim().length > 0
          ? nickname.trim()
          : `dev_${resolvedProviderId}`;

      user = await prisma.users.create({
        data: {
          provider: "GOOGLE",
          providerId: resolvedProviderId,
          nickname: resolvedNickname,
        },
      });
    }

    const sessionUser = {
      id: user.id.toString(),
      name: user.nickname ?? null,
      email: null,
      provider: String(user.provider).toLowerCase(),
    };

    return req.logIn(sessionUser, (loginErr) => {
      if (loginErr) {
        return res.status(500).json({
          ok: false,
          message: "세션 생성 실패",
        });
      }

      return buildAuthResponseUserById(user.id)
        .then((authUser) =>
          res.status(200).json({
            ok: true,
            message: "로컬 세션 로그인 성공",
            user: authUser,
          })
        )
        .catch((authErr) => {
          console.error(authErr);
          return res.status(500).json({
            ok: false,
            message: "로컬 세션 로그인 실패",
          });
        });
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({
      ok: false,
      message: "로컬 세션 로그인 실패",
    });
  }
});

app.post("/api/auth/logout", (req, res) => {
  req.logout((logoutErr) => {
    if (logoutErr) {
      return res.status(500).json({
        ok: false,
        message: "로그아웃 처리 실패",
      });
    }

    if (!req.session) {
      res.clearCookie("connect.sid", clearCookieOptions);
      return res.status(200).json({
        ok: true,
        message: "로그아웃되었습니다.",
      });
    }

    return req.session.destroy((sessionErr) => {
      if (sessionErr) {
        return res.status(500).json({
          ok: false,
          message: "세션 종료 실패",
        });
      }

      res.clearCookie("connect.sid", clearCookieOptions);
      return res.status(200).json({
        ok: true,
        message: "로그아웃되었습니다.",
      });
    });
  });
});

app.use("/api/oauth2", oauthRouter)
app.use("/api/users", userRoutes)
app.use("/api/videos", videoRoutes);
app.use("/api/v1", v1Routes);


// 테스트용 헬스 체크 API
app.get('/api/test', (req, res) => {
  res.status(200).json({
    ok: true,
    message: 'test api response',
    timestamp: new Date().toISOString()
  });
});

app.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
});
