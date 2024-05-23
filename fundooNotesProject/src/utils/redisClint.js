import redis from 'ioredis';
import dotenv from 'dotenv';
dotenv.config();

const redisClint = redis.createClient(process.env.REDIS_PORT);

export async function clearRedisClint(userId) {
  await redisClint.del(`user:${userId}`);
}

export async function setToRedisClint(userId, notes) {
  const noteList = Array.isArray(notes) ? notes : [notes];
  for (let note of noteList) {
    await redisClint.lpush(`user:${userId}`, JSON.stringify(note));
  }
}

export async function getAllNotesFromRedisClint(userId) {
  const getAllNotes = await redisClint.lrange(`user:${userId}`, 0, -1);
  const notesArray = Object.values(getAllNotes).map(JSON.parse);
  return notesArray;
}

export async function getNoteFromRedisClint(userId, noteId) {
  const getAllNotes = await redisClint.lrange(`user:${userId}`, 0, -1);
  const notesArray = Object.values(getAllNotes).map(JSON.parse);
  return notesArray.find((note) => {
    return note._id === noteId;
  });
}

export async function deleteNoteFromRedisClint(userId, noteId) {
  const note = await getNoteFromRedisClint(userId, noteId);
  await redisClint.lrem(`user:${userId}`, 0, JSON.stringify(note));
}
