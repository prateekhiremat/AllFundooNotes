import request from 'supertest';
import mongoose from 'mongoose';
import app from '../../src/index';
import dotenv from 'dotenv';
dotenv.config();

let loginToken;
let resetToken;
let noteId;

describe('API testing', () => {
  beforeAll((done) => {
    const clearCollections = () => {
      for (const collection in mongoose.connection.collections) {
        mongoose.connection.collections[collection].deleteOne(() => {});
      }
    };
    const mongooseConnect = async () => {
      await mongoose.connect(process.env.DATABASE_TEST);
      clearCollections();
    };
    if (mongoose.connection.readyState === 0) {
      mongooseConnect();
    } else {
      clearCollections();
    }
    done();
  });

  afterAll(async () => {
    await mongoose.connection.close();
  });

  describe('New User', () => {
    it('Should not register user', async () => {
      const res = await request(app).post('/api/users').send({
        firstName: 'Harsh',
        lastName: '',
        email: 'harsh@email.com',
        password: 'Harsh@123'
      });
      expect(res.status).toBe(400);
      expect(res.body.success).toBe(false);
    });

    it('Should register user', async () => {
      const res = await request(app).post('/api/users').send({
        firstName: 'Prateek',
        lastName: 'Hiremath',
        email: 'prateek.s.hiremath123@gmail.com',
        password: 'Prateek@123'
      });
      expect(res.status).toBe(201);
      expect(res.body.success).toBe(true);
    });

    it('Should not register user', async () => {
      const res = await request(app).post('/api/users').send({
        firstName: 'Prateek',
        lastName: 'Hiremath',
        email: 'prateek.s.hiremath123@gmail.com',
        password: 'Prateek@123'
      });
      expect(res.status).toBe(400);
      expect(res.body.success).toBe(false);
    });
  });

  describe('Login User', () => {
    it('Should login', async () => {
      const res = await request(app).post('/api/users/login').send({
        email: 'prateek.s.hiremath123@gmail.com',
        password: 'Prateek@123'
      });
      loginToken = res.body.token;
      expect(res.status).toBe(200);
      expect(res.body.success).toBe(true);
      expect(res.body.email).toBeDefined();
      expect(res.body.firstName).toBeDefined();
      expect(res.body.token).toBeDefined();
    });

    it('Should not login', async () => {
      const res = await request(app).post('/api/users/login').send({
        email: 'appu@email.com',
        password: 'Appu@123'
      });
      expect(res.status).toBe(400);
      expect(res.body.success).toBe(false);
      expect(res.body.email).not.toBeDefined();
    });
  });

  describe('Forgot Password', () => {
    it('Should send mail along with token', async () => {
      const res = await request(app)
        .post('/api/users/forgotpassword')
        .send({
          email: 'prateek.s.hiremath123@gmail.com'
        })
        .expect(200);
      expect(res.body.success).toBe(true);
      resetToken = res.body.token;
    });

    it('Should not send mail along with token', async () => {
      const res = await request(app)
        .post('/api/users/forgotpassword')
        .send({
          email: 'appu@gmail.com'
        })
        .expect(400);
      expect(res.body.success).toBe(false);
    });
  });

  describe('Reset Password', () => {
    it('Should reset the password', async () => {
      const res = await request(app)
        .post('/api/users/resetPassword')
        .set('Authorization', `Bearer ${resetToken}`)
        .send({ password: 'Hiremath@987' });
      expect(res.statusCode).toBe(200);
      expect(res.body.success).toBe(true);
    });

    it('Should not reset the password', async () => {
      const res = await request(app)
        .post('/api/users/resetPassword')
        .set('Authorization', `Bearer ${resetToken}`)
        .send({ password: '' });
      expect(res.statusCode).toBe(400);
      expect(res.body.success).toBe(false);
    });
  });

  describe('Create Note', () => {
    it('Should create a new note', async () => {
      const res = await request(app)
        .post('/api/notes')
        .set('Authorization', `Bearer ${loginToken}`)
        .send({
          title: 'Test Note',
          description: 'This is a test note',
          color: 'yellow'
        });
      expect(res.status).toBe(201);
      expect(res.body.success).toBe(true);
      expect(res.body.id).toBeDefined();
      noteId = res.body.id;
    });

    it('Should not create note', async () => {
      const res = await request(app)
        .post('/api/notes')
        .set('Authorization', `Bearer ${loginToken}`)
        .send({
          title: '',
          description: 'jest',
          color: 'red'
        });
      expect(res.status).toBe(400);
    });
  });

  describe('Get all Note', () => {
    it('Should fetch notes', async () => {
      const res = await request(app)
        .get('/api/notes')
        .set('Authorization', `Bearer ${loginToken}`);
      expect(res.status).toBe(200);
      expect(res.body.success).toBe(true);
    });

    it('Should not fetch notes', async () => {
      const res = await request(app)
        .get('/api/notes')
        .set('Authorization', `Bearer ${loginToken}0`);
      expect(res.status).toBe(400);
      expect(res.body.success).toBe(false);
    });
  });

  describe('Get single Note', () => {
    it('Should get a note', async () => {
      const res = await request(app)
        .get(`/api/notes/${noteId}`)
        .set('Authorization', `Bearer ${loginToken}`);
      expect(res.status).toBe(200);
      expect(res.body.success).toBe(true);
    });

    it('Should not get a note', async () => {
      const res = await request(app)
        .get(`/api/notes/${noteId}0`)
        .set('Authorization', `Bearer ${loginToken}`);
      expect(res.status).toBe(400);
      expect(res.body.success).toBe(false);
    });

    it('Should not fetch a note', async () => {
      const res = await request(app)
        .get(`/api/notes/${noteId}`)
        .set('Authorization', `Bearer ${loginToken}0`);
      expect(res.status).toBe(400);
      expect(res.body.success).toBe(false);
    });
  });

  describe('Update Note', () => {
    it('Should update an existing note', async () => {
      const res = await request(app)
        .put(`/api/notes/${noteId}`)
        .send({
          title: 'To Do',
          description: 'Testing using jest',
          color: 'black'
        })
        .set('Authorization', `Bearer ${loginToken}`);
      expect(res.status).toBe(200);
      expect(res.body.success).toBe(true);
    });

    it('Should not update note', async () => {
      const res = await request(app)
        .put(`/api/notes/${noteId}0`)
        .send({
          title: 'To Do',
          description: 'Testing using jest',
          color: 'black'
        })
        .set('Authorization', `Bearer ${loginToken}`);
      expect(res.status).toBe(400);
      expect(res.body.success).toBe(false);
    });

    it('Should not update note', async () => {
      const res = await request(app)
        .put(`/api/notes/${noteId}`)
        .send({
          title: 'To Do',
          description: 'Testing using jest',
          color: 'black'
        })
        .set('Authorization', `Bearer ${loginToken}0`);
      expect(res.status).toBe(400);
      expect(res.body.success).toBe(false);
    });

    it('Should update an existing note', async () => {
      const res = await request(app)
        .put(`/api/notes/${noteId}`)
        .send({
          title: 'To Do Testing Verification',
          color: 'black'
        })
        .set('Authorization', `Bearer ${loginToken}`);
      expect(res.status).toBe(200);
      expect(res.body.success).toBe(true);
    });
  });

  describe('archive a Note()', () => {
    it('Should toggle isArchive in note', async () => {
      const res = await request(app)
        .post(`/api/notes/isArchived/${noteId}`)
        .set('Authorization', `Bearer ${loginToken}`);
      expect(res.status).toBe(200);
      expect(res.body.success).toBe(true);
    });
  });

  describe('trash a Note()', () => {
    it('Should toggle isTrached in note', async () => {
      const res = await request(app)
        .post(`/api/notes/isTrashed/${noteId}`)
        .set('Authorization', `Bearer ${loginToken}`);
      expect(res.status).toBe(200);
      expect(res.body.success).toBe(true);
    });
  });

  describe('delete a Note()', () => {
    it('Should delete a note', async () => {
      const res = await request(app)
        .delete(`/api/notes/${noteId}`)
        .set('Authorization', `Bearer ${loginToken}`);
      expect(res.status).toBe(200);
      expect(res.body.success).toBe(true);
    });
  });
});
