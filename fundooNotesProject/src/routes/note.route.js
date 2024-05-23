import express from 'express';
import * as noteController from '../controllers/note.controller';
import * as validator from '../validators/note.validator';
import { userAuth } from '../middlewares/auth.middleware';

const router = express.Router();

/* route to notes creation */
router.post('', userAuth, validator.noteValidator, noteController.createNotes);

router.get('', userAuth, noteController.getAllNotes);

router.get('/:_id', userAuth, noteController.getNote);

router.put('/:_id', userAuth, noteController.updateNote);

router.delete('/:_id', userAuth, noteController.deleteNote);

router.post('/isArchived/:_id', userAuth, noteController.isArchived);

router.post('/isTrashed/:_id', userAuth, noteController.isTrashed);

export default router;
