import { ConsumeMessage, Channel } from 'amqplib'
import { IMessageBroker } from './MessageBroker'

class MockMessageBroker implements IMessageBroker {
  public async sendToQueue(queueName: string, message: Buffer): Promise<void> {
    return
  }

  // tslint:disable-next-line:no-any
  public async consume(queueName: string, cb: (cm: ConsumeMessage, channel: Channel) => any): Promise<void> {
    return
  }

  public async closeConnection(): Promise<void> {
    return
  }
}

export default MockMessageBroker
