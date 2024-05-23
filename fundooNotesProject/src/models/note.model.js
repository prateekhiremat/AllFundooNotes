import { Schema, model } from 'mongoose';

const noteSchema = new Schema(
  {
    title: {
      type: String
    },
    description: {
      type: String
    },
    color: {
      type: String,
      default: 'white'
    },
    isArchived: {
      type: Boolean,
      default: false
    },
    isTrashed: {
      type: Boolean,
      default: false
    },
    createdBy: {
      type: Schema.Types.ObjectId,
      ref: 'User' // Name of the referenced model
    }
  },
  {
    timestamps: true
  }
);

export default model('Note', noteSchema);
