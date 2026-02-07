import jwt from 'jsonwebtoken';
import { JWT_SECRET } from '../app.js';
import {
  findUserByEmail,
  createUser
} from '../../services/userService.js';

// 로그인
export async function login(req, res) {
  const { email, password } = req.body;

  if (!email || !password) {
    return res.status(400).json({
      ok: false,
      message: '이메일과 비밀번호를 입력해주세요.'
    });
  }

  const user = await findUserByEmail(email);

  if (!user || user.password !== password) {
    return res.status(401).json({
      ok: false,
      message: '이메일 또는 비밀번호가 올바르지 않습니다.'
    });
  }

  const token = jwt.sign(
    {
      id: user.id,
      email: user.email
    },
    JWT_SECRET,
    { expiresIn: '1h' }
  );

  return res.json({
    ok: true,
    user: {
      id: user.id,
      email: user.email
    },
    token
  });
}

// 회원가입
export async function signup(req, res) {
  const { email, password } = req.body;

  if (!email || !password) {
    return res.status(400).json({
      ok: false,
      message: '이메일과 비밀번호는 필수입니다.'
    });
  }

  if (password.length < 4) {
    return res.status(400).json({
      ok: false,
      message: '비밀번호는 최소 4자 이상이어야 합니다.'
    });
  }

  const exists = await findUserByEmail(email);
  if (exists) {
    return res.status(409).json({
      ok: false,
      message: '이미 사용 중인 이메일입니다.'
    });
  }

  const newUser = await createUser({ email, password });

  return res.status(201).json({
    ok: true,
    message: '회원가입이 완료되었습니다.',
    user: {
      id: newUser.id,
      email: newUser.email
    }
  });
}
