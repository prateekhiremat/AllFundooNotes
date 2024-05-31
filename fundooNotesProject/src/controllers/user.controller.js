import HttpStatus from 'http-status-codes';
import * as UserService from '../services/user.service';
import { init } from '../utils/kafka/producer';
import { consumerInit } from '../utils/kafka/consumer';

export const userRegister = async (req, res) => {
  try {
    // await init(req.body)
    // await consumerInit().then(data => {
    //   console.log('Received data:', data);
    // })
    // .catch(error => {
    //   console.error('Error:', error);
    // });
    await UserService.userRegister(req.body);
    res.status(HttpStatus.CREATED).json({
      success: true,
      message: 'User created successfully'
    });
  } catch (error) {
    res.status(HttpStatus.BAD_REQUEST).json({
      success: false,
      message: `${error}`
    });
  }
};

export const userLogin = async (req, res) => {
  try {
    const data = await UserService.userLogin(req.body);
    const { firstName, email } = data.user;
    res.status(HttpStatus.OK).json({
      success: true,
      message: 'User loggedIn successfully',
      firstName,
      email,
      token: data.token
    });
  } catch (error) {
    res.status(HttpStatus.BAD_REQUEST).json({
      success: false,
      message: `${error}`
    });
  }
};

export const forgetPassword = async (req, res) => {
  try {
    const data = await UserService.forgetPassword(req.body.email);
    res.status(HttpStatus.OK).json({
      success: true,
      message: 'Please reset your password through email',
      token: data
    });
  } catch (error) {
    res.status(HttpStatus.BAD_REQUEST).json({
      success: false,
      message: `${error}`
    });
  }
};

export const resetPassword = async (req, res) => {
  try {
    await UserService.resetPassword(req.userId, req.body.password);
    res.status(HttpStatus.OK).json({
      success: true,
      message: 'Password reset successfull'
    });
  } catch (error) {
    res.status(HttpStatus.BAD_REQUEST).json({
      success: false,
      message: `${error}`
    });
  }
};
