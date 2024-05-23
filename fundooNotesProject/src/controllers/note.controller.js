import HttpStatus from 'http-status-codes';
import * as NoteService from '../services/note.service';

export const createNotes = async (req, res) => {
  try {
    const data = await NoteService.createNotes(req.body.createdBy, req.body);
    res.status(HttpStatus.CREATED).json({
      success: true,
      message: 'Note created successfully',
      id: data
    });
  } catch (error) {
    res.status(HttpStatus.BAD_REQUEST).json({
      success: false,
      message: `${error}`
    });
  }
};

export const getAllNotes = async (req, res) => {
  try {
    const data = await NoteService.getAllNotes(req.body.createdBy);
    const resData = data.map(({ title, description, color }) => ({
      title,
      description,
      color
    }));
    res.status(HttpStatus.OK).json({
      success: true,
      message: 'fetched successfully',
      data: resData
    });
  } catch (error) {
    res.status(HttpStatus.BAD_REQUEST).json({
      success: false,
      message: `${error}`
    });
  }
};

export const getNote = async (req, res) => {
  try {
    const data = await NoteService.getNote(req.params._id, req.body.createdBy);
    const { title, description, color } = data;
    res.status(HttpStatus.OK).json({
      success: true,
      message: 'Note fetched successfully',
      title,
      description,
      color
    });
  } catch (error) {
    res.status(HttpStatus.BAD_REQUEST).json({
      success: false,
      message: `${error}`
    });
  }
};

export const updateNote = async (req, res) => {
  try {
    await NoteService.updateNote(req.params._id, req.body, req.body.createdBy);
    res.status(HttpStatus.OK).json({
      success: true,
      message: 'Note Updated Successfully'
    });
  } catch (error) {
    res.status(HttpStatus.BAD_REQUEST).json({
      success: false,
      message: `${error}`
    });
  }
};

export const deleteNote = async (req, res) => {
  try {
    await NoteService.deleteNote(req.params._id, req.body.createdBy);
    res.status(HttpStatus.OK).json({
      success: true,
      message: 'Note Deleated Successfully'
    });
  } catch (error) {
    res.status(HttpStatus.BAD_REQUEST).json({
      success: false,
      message: `${error}`
    });
  }
};

export const isArchived = async (req, res) => {
  try {
    await NoteService.isArchived(req.params._id, req.body.createdBy);
    res.status(HttpStatus.OK).json({
      success: true,
      message: 'Note updated successfully'
    });
  } catch (error) {
    res.status(HttpStatus.BAD_REQUEST).json({
      success: false,
      message: `${error}`
    });
  }
};

export const isTrashed = async (req, res) => {
  try {
    await NoteService.isTrashed(req.params._id, req.body.createdBy);
    res.status(HttpStatus.OK).json({
      success: true,
      message: 'Note updated successfully'
    });
  } catch (error) {
    res.status(HttpStatus.BAD_REQUEST).json({
      success: false,
      message: `${error}`
    });
  }
};
