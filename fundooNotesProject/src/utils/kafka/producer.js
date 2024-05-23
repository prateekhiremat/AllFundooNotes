import logger from '../../config/logger.js';
import kafka from '../kafka/client.js';

export async function init(user) {
  const producer = kafka.producer();
  await producer.connect();
  logger.info(`Producer Connected Successfully`);
  await producer.send({
    topic: 'User',
    messages: [
      {
        partition: 0,
        key: 'User-Info',
        value: JSON.stringify(user)
      }
    ]
  });
  logger.info('Disconnecting Producer...')
  await producer.disconnect();
}
