import kafka from './client.js';
import logger from '../../config/logger.js';

export async function init() {
  const admin = kafka.admin();
  admin.connect();
  logger.info('Admin Connected Successfully');
  await admin.createTopics({
    topics: [
      {
        topic: 'User',
        numPartitions: 1
      },
      {
        topic: 'Note',
        numPartitions: 1
      }
    ]
  });
  logger.info('Topics Created Successfully');
  logger.info('Admin Disconnecting...');
  admin.disconnect();
}
