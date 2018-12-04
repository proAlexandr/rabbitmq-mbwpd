import { ConsumeMessage, Channel } from "amqplib";

interface IMessageBroker {
  sendToQueue(queueName: string, message: Buffer): Promise<void>
  // tslint:disable-next-line:no-any
  consume(queueName: string, cb: (cm: ConsumeMessage, channel: Channel) => any): Promise<void>
  closeConnection(): Promise<void>
}

export default IMessageBroker