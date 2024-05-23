import dotenv from 'dotenv';
dotenv.config();

import jwt from 'jsonwebtoken';
const secretKey = process.env.SECRET_KEY;
const secretKeyReset = process.env.SECRET_KEY_RESET;

export function generateToken(userId) {
  const payload = {
    _id: userId
  };
  return jwt.sign(payload, secretKey, { expiresIn: '1d' });
}

export function generateTokenForPassword(userId) {
  const payload = {
    _id: userId
  };
  return jwt.sign(payload, secretKeyReset, { expiresIn: '5m' });
}
