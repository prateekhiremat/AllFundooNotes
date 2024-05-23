import Joi from '@hapi/joi';
import HttpStatus from 'http-status-codes';

export const noteValidator = (req, res, next) => {
  const schema = Joi.object({
    title: Joi.string().required(),
    description: Joi.string().required(),
    color: Joi.string(),
    createdBy: Joi.string().required()
  });
  const { error, value } = schema.validate(req.body);
  if (error)
    res.status(HttpStatus.BAD_REQUEST).json({
      success: false,
      message: `${error}`
    });
  else next();
};
