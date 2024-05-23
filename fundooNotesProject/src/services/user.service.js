import User from '../models/user.model';
import bcrypt from 'bcrypt';
import * as userUtility from '../utils/user.util';
import sendMail from '../utils/emailService.util';
import { getAllNotesForRedis } from './note.service';
import { clearRedisClint, setToRedisClint } from '../utils/redisClint';

export const userRegister = async (body) => {
  body.email = body.email.toLowerCase();
  await User.findOne({ email: body.email })
    .then((userObj) => {
      if (userObj !== null) {
        throw new Error('User Already Exist');
      }
      return bcrypt.hash(body.password, 10);
    })
    .then((hashedPassword) => {
      body.password = hashedPassword;
      User.create(body);
    });
};

export const userLogin = async (body) => {
  body.email = body.email.toLowerCase();
  return User.findOne({ email: body.email })
    .then((userObj) => {
      if (userObj === null) throw new Error('Invalid email');
      return userObj;
    })
    .then((userObj) => {
      return new Promise((resolve, reject) => {
        bcrypt.compare(
          body.password,
          userObj.password,
          async function (err, result) {
            if (result) {
              const token = userUtility.generateToken(userObj._id);
              const redisData = await getAllNotesForRedis(userObj._id);
              clearRedisClint(userObj._id);
              setToRedisClint(userObj._id, redisData);
              resolve({ user: userObj, token });
            } else reject(new Error('Invalid password'));
          }
        );
      });
    });
};

export const forgetPassword = async (email) => {
  const user = await User.findOne({ email });
  if (user !== null) {
    const token = userUtility.generateTokenForPassword(user._id);
    const subject = 'Reset Password';
    const message = `<h1>http://localhost:3000/api/users/reset-password</h1>\n${token}`;
    sendMail(user.email, subject, message);
    return token;
  } else {
    throw new Error('Unathorized Request');
  }
};

export const resetPassword = async (userId, newPassword) => {
  const user = await User.findOne({ _id: userId });
  if (user === null) throw new Error('Unathorized Request');
  user.password = await bcrypt.hash(newPassword, 10);
  await User.findOneAndUpdate(
    {
      _id: user._id
    },
    user,
    {
      new: true
    }
  );
};
