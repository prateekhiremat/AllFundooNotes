import Joi from '@hapi/joi';
import HttpStatus from 'http-status-codes';

export const userValidatorRegister = (req, res, next) => {
  const schema = Joi.object({
    firstName: Joi.string().required(),
    lastName: Joi.string().required(),
    email: Joi.string().lowercase().email().required(),
    password: Joi.string()
      .regex(/^(?=.*[!@#$%^&*])(?=.*[a-z])(?=.*[A-Z])(?=.*[0-9]).{8,}$/)
      .message(
        'password must contain atleast 8 character with one special, number, upper & lowercase character'
      )
      .required()
  });
  const { error, value } = schema.validate(req.body);
  if (error) {
    res.status(HttpStatus.BAD_REQUEST).json({
      success: false,
      message: `${error}`
    });
  } else {
    next();
  }
};

export const loginRegister = (req, res, next) => {
  const schema = Joi.object({
    email: Joi.string().lowercase().email().required(),
    password: Joi.string()
      .regex(/^(?=.*[!@#$%^&*])(?=.*[a-z])(?=.*[A-Z])(?=.*[0-9]).{8,}$/)
      .message(
        'password must contain atleast 8 character with one special, number, upper & lowercase character'
      )
      .required()
  });
  const { error, value } = schema.validate(req.body);
  if (error) {
    res.status(HttpStatus.BAD_REQUEST).json({
      success: false,
      message: `${error}`
    });
  } else {
    next();
  }
};

export const email = (req, res, next) => {
  const schema = Joi.object({
    email: Joi.string().lowercase().email().required()
  });
  const { error, value } = schema.validate(req.body);
  if (error) {
    res.status(HttpStatus.BAD_REQUEST).json({
      success: false,
      message: `${error}`
    });
  } else {
    next();
  }
};

export const password = (req, res, next) => {
  const schema = Joi.object({
    password: Joi.string()
      .regex(/^(?=.*[!@#$%^&*])(?=.*[a-z])(?=.*[A-Z])(?=.*[0-9]).{8,}$/)
      .message(
        'password must contain atleast 8 character with one special, number, upper & lowercase character'
      )
      .required()
  });
  const { error, value } = schema.validate(req.body);
  if (error) {
    res.status(HttpStatus.BAD_REQUEST).json({
      success: false,
      message: `${error}`
    });
  } else {
    next();
  }
};
