import express from 'express';
import dotenv from "dotenv"
import cors from 'cors';
import authRouter from './routes/auth.js';
import { authMiddleware } from './middlewares/authMiddleware.js';
import adminRoutes from "./routes/adminRoutes.js";
import uploadRoutes from "./routes/uploadRoutes.js";

dotenv.config();

const app = express();
const PORT = process.env.PORT;

// ⚠️ 나중에 .env로 빼면 됨
export const JWT_SECRET = 'DEV_SECRET_KEY';

app.use(cors());
app.use(express.static('public')); 
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

app.use('/api/auth', authRouter);
app.use("/api/admin", adminRoutes);

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

app.use("/api/uploads", uploadRoutes);
