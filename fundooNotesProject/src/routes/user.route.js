import express from 'express';
import * as userController from '../controllers/user.controller';
import * as validator from '../validators/user.validator';
import { userAuthForResetPassword } from '../middlewares/auth.middleware';

const router = express.Router();

/* route to user registration */
router.post('/register', validator.userValidatorRegister, userController.userRegister);

router.post('/login', validator.loginRegister, userController.userLogin);

router.post('/forgotpassword', validator.email, userController.forgetPassword);

router.post(
  '/resetPassword',
  validator.password,
  userAuthForResetPassword,
  userController.resetPassword
);

export default router;
