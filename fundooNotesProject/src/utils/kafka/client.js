import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'fundoo-app',
  brokers: ['localhost:9092']
});
export default kafka;
