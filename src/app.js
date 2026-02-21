import express from 'express';
import dotenv from "dotenv"
import cors from 'cors';
import authRouter from './routes/auth.js';
import { authMiddleware } from './middlewares/authMiddleware.js';
import adminRoutes from "./routes/adminRoutes.js";
import uploadRoutes from "./routes/uploadRoutes.js";
import oauthRouter from "./routes/oauthRouter.js";
import { PrismaSessionStore } from "@quixo3/prisma-session-store";
import session from "express-session";
import passport from "passport";
import { googleStrategy } from "./auth.config.js";
import { prisma } from "./db.config.js";

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
      provider: user.provider,
      providerId: user.providerId,
      nickname: user.nickname,
    });
  } catch (err) {
    return done(err);
  }
});

const app = express();
const PORT = process.env.PORT;

// ⚠️ 나중에 .env로 빼면 됨
export const JWT_SECRET = 'DEV_SECRET_KEY';

app.use(cors());
app.use(express.static('public')); 
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(
  session({
    cookie: {
      maxAge: 7 * 24 * 60 * 60 * 1000, // ms
    },
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

app.use('/api/auth', authRouter);
app.use("/api/admin", adminRoutes);
app.use("/api/oauth2", oauthRouter)
app.use("/api/uploads", uploadRoutes);


// 테스트용 헬스 체크 API
app.get('/api/test', (req, res) => {
  res.status(200).json({
    ok: true,
    message: 'test api response',
    timestamp: new Date().toISOString()
  });
});

// 🔐 로그인된 사용자만 접근 가능
app.get('/api/me', authMiddleware, (req, res) => {
  res.json({
    ok: true,
    user: req.user
  });
});

app.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
});
