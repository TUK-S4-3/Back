import { GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { s3 } from "../utils/s3.js";
import { streamToString } from "../utils/stream.js";

const BUCKET = process.env.AWS_S3_BUCKET;
const KEY = "data/users.json";

// 전체 유저 불러오기
async function getUsers() {
  try {
    const command = new GetObjectCommand({
      Bucket: BUCKET,
      Key: KEY,
    });

    const data = await s3.send(command);
    const body = await streamToString(data.Body);
    return JSON.parse(body);
  } catch (err) {
    // 파일 없으면 초기 상태
    if (err.name === "NoSuchKey") {
      return [];
    }
    throw err;
  }
}

// 전체 유저 저장
async function saveUsers(users) {
  const command = new PutObjectCommand({
    Bucket: BUCKET,
    Key: KEY,
    Body: JSON.stringify(users, null, 2),
    ContentType: "application/json",
  });

  await s3.send(command);
}

// 이메일로 유저 찾기
export async function findUserByEmail(email) {
  const users = await getUsers();
  return users.find(user => user.email === email);
}

// 유저 생성
export async function createUser({ email, password, name }) {
  const users = await getUsers();

  const exists = users.find(u => u.email === email);
  if (exists) {
    throw new Error("USER_ALREADY_EXISTS");
  }

  const newUser = {
    id: Date.now(),
    email,
    password, // ⚠️ 과제용 (실무면 bcrypt 필수)
    name,
    createdAt: new Date().toISOString(),
  };

  users.push(newUser);
  await saveUsers(users);

  return newUser;
}
