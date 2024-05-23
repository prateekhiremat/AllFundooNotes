import Note from '../models/note.model';
import {
  deleteNoteFromRedisClint,
  getAllNotesFromRedisClint,
  getNoteFromRedisClint,
  setToRedisClint
} from '../utils/redisClint';

export const createNotes = async (userId, body) => {
  const note = await Note.create(body);
  await setToRedisClint(userId, note);
  return note._id;
};

export const getAllNotes = async (_id) => {
  return await getAllNotesFromRedisClint(_id);
};

export const getNote = async (_id, userId) => {
  const note = await getNoteFromRedisClint(userId, _id);
  if (note !== null) return note;
  throw new Error('Note not found');
};

export const updateNote = async (_id, body, userId) => {
  let note = await getNoteFromRedisClint(userId, _id);
  if (note === null) throw new Error('Note not found');
  note = await Note.findOneAndUpdate(
    {
      _id,
      createdBy: userId
    },
    body,
    {
      new: true
    }
  );
  await deleteNoteFromRedisClint(userId, _id);
  await setToRedisClint(userId, note);
};

export const deleteNote = async (_id, userId) => {
  const note = await getNoteFromRedisClint(userId, _id);
  if (note === null) throw new Error('Note not found');
  await Note.findOneAndDelete({
    _id,
    createdBy: userId
  });
  await deleteNoteFromRedisClint(userId, _id);
};

export const isArchived = async (_id, userId) => {
  let note = await getNoteFromRedisClint(userId, _id);
  if (note === null) throw new Error('Note not found');
  note.isArchived = !note.isArchived;
  note = await Note.findByIdAndUpdate(
    {
      _id
    },
    note,
    {
      new: true
    }
  );
  await deleteNoteFromRedisClint(userId, _id);
  await setToRedisClint(userId, note);
};

export const isTrashed = async (_id, userId) => {
  let note = await getNoteFromRedisClint(userId, _id);
  if (note === null) throw new Error('Note not found');
  note.isTrashed = !note.isTrashed;
  note = await Note.findByIdAndUpdate(
    {
      _id
    },
    note,
    {
      new: true
    }
  );
  await deleteNoteFromRedisClint(userId, _id);
  await setToRedisClint(userId, note);
};

export const getAllNotesForRedis = async (_id) => {
  return Note.find({ createdBy: _id });
};
