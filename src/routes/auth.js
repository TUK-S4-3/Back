import express from 'express';
import { login, signup } from '../src/controllers/authController.js';

const router = express.Router();

// 로그인
router.post('/login', login);

// 회원가입
router.post('/signup', signup);

export default router;
