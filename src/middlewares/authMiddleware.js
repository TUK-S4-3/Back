import jwt from 'jsonwebtoken';
import { JWT_SECRET } from '../app.js';

export function authMiddleware(req, res, next) {
  const authHeader = req.headers.authorization;

  if (!authHeader) {
    return res.status(401).json({
      ok: false,
      message: '인증 토큰이 없습니다.'
    });
  }

  const [type, token] = authHeader.split(' ');

  if (type !== 'Bearer' || !token) {
    return res.status(401).json({
      ok: false,
      message: '토큰 형식이 올바르지 않습니다.'
    });
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET);

    // 👉 이후 라우터에서 req.user로 접근 가능
    req.user = {
      id: decoded.id,
      email: decoded.email,
      role: decoded.role
    };

    next();
  } catch (err) {
    return res.status(401).json({
      ok: false,
      message: '유효하지 않은 토큰입니다.'
    });
  }
}
