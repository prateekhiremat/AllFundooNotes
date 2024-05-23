import HttpStatus from 'http-status-codes';
import jwt from 'jsonwebtoken';
import dotenv from 'dotenv';
dotenv.config();

const secretKey = process.env.SECRET_KEY;
const secretKeyReset = process.env.SECRET_KEY_RESET;

/**
 * Middleware to authenticate if user has a valid Authorization token
 * Authorization: Bearer <token>
 *
 * @param {Object} req
 * @param {Object} res
 * @param {Function} next
 */
export const userAuth = async (req, res, next) => {
  try {
    let bearerToken = req.header('Authorization');
    if (!bearerToken)
      throw {
        code: HttpStatus.BAD_REQUEST,
        message: 'Please login'
      };
    bearerToken = bearerToken.split(' ')[1];
    const userPayload = jwt.verify(bearerToken, secretKey);
    req.body.createdBy = userPayload._id;
    next();
  } catch (error) {
    res.status(HttpStatus.BAD_REQUEST).json({
      success: false,
      message: `${error}`
    });
  }
};

export const userAuthForResetPassword = async (req, res, next) => {
  try {
    let bearerToken = req.header('Authorization');
    if (!bearerToken)
      throw {
        code: HttpStatus.BAD_REQUEST,
        message: 'Bad Request'
      };
    bearerToken = bearerToken.split(' ')[1];
    const userPayload = jwt.verify(bearerToken, secretKeyReset);
    req.userId = userPayload._id;
    next();
  } catch (error) {
    next(error);
  }
};
