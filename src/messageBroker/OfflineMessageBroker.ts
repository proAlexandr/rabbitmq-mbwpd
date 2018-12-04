import { ConsumeMessage, Channel } from 'amqplib'
import IMessageBroker from './IMessageBroker'

interface ISavedQueue {
  queueName: string
  message: Buffer
}

class OfflineMessageBroker implements IMessageBroker {
  private savedQueue: ISavedQueue[]
  private LIMIT: number = 1000

  constructor() {
    this.savedQueue = []
  }

  public static IS_OFFLINE_MESSABE_BROKER(messageBroker: IMessageBroker): messageBroker is OfflineMessageBroker {
    return messageBroker instanceof OfflineMessageBroker
  }

  public async sendToQueue(queueName: string, message: Buffer): Promise<void> {
    if (this.savedQueue.length > this.LIMIT) {
      this.savedQueue.shift()
    }
    this.savedQueue.push({ queueName, message })
  }

  // tslint:disable-next-line:no-any
  public async consume(queueName: string, cb: (cm: ConsumeMessage, channel: Channel) => any): Promise<void> {
    const queueByName: ISavedQueue[] = this.savedQueue.filter((saved: ISavedQueue) => saved.queueName === queueName)

    // tslint:disable-next-line:no-any
    await Promise.all(queueByName.map((saved: ISavedQueue) => cb({ content: saved } as any, null as any)))
  }

  public async closeConnection(): Promise<void> {
    this.savedQueue = []
  }
}

export default OfflineMessageBroker
