import kafka from './client';
import { EventEmitter } from 'events';

export async function consumerInit() {
  const consumer = kafka.consumer({ groupId: 'user-1' });
  const emitter = new EventEmitter();
  
  await consumer.connect();
  await consumer.subscribe({ topics: ['User'], fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const data = {
        value: message.value.toString(),
        topic,
        partition
      };
      console.log(`[${topic}] : PART:${partition}: ${message.value.toString()}`);
      console.log('Received message:', data);
      emitter.emit('data', data);
    }
  });

  return new Promise((resolve, reject) => {
    emitter.once('data', (data) => {
      resolve(data);
    });
    // Set timeout to reject promise if no data received after 10 seconds
    setTimeout(() => {
      reject(new Error('No data received'));
    }, 10000);
  });
}

/* import kafka from '../kafka/client.js';

export async function consumerInit() {
  const consumer = kafka.consumer({ groupId: 'user-1' });
  await consumer.connect()
  await consumer.subscribe({ topics: ['User'], fromBeginning: false })
  let consumerData = [];
  await consumer.run({
    eachMessage: ({ topic, partition, message }) => {
      const data = {
        value: message.value.toString(),
        topic,
        partition
      }
      console.log('*************************************')
      console.log('inner',consumerData)
      console.log(`[${topic}] : PART:${partition}: ${message.value.toString()}`);
      consumerData.push(data)
    }
  })
  return consumerData
} */
