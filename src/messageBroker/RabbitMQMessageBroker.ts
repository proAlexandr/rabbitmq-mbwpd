import { Connection, Channel, connect, Replies, ConsumeMessage } from 'amqplib'
import IMessageBroker from './IMessageBroker'

interface ISavedQueue {
  queueName: string
  message: Buffer
}

// all queues have { durable: true, noAck: false }. Dont forget to do `channel.ack(msg)` in consume callback
class RabbitMQMessageBroker implements IMessageBroker {
  private configUrl: string
  private connection: Connection | null
  private channel: Channel | null
  private errorCallback: (error?: Error) => void
  private savedMessagesWaitingForDrainEvent: ISavedQueue[]

  constructor(configUrl: string, errorCallback: () => void = (): void => { return }) {
    this.configUrl = configUrl
    this.errorCallback = errorCallback
  }

  public static IS_RABBITMQ_MESSAGE_BROKER(messageBroker: IMessageBroker): messageBroker is RabbitMQMessageBroker {
    return messageBroker instanceof RabbitMQMessageBroker
  }

  public async getChannel(): Promise<Channel> {
    if (!this.channel) {
      await this.init()
    }

    return this.channel as Channel
  }

  public async sendToQueue(queueName: string, message: Buffer): Promise<void> {
    const channel: Channel = await this.getChannel()
    const { queue }: Replies.AssertQueue = await channel.assertQueue(queueName, { durable: true })

    const isSended: boolean = channel.sendToQueue(queue, message)
    if (!isSended) {
      this.handleMessageNotSendDrain({ queueName: queue, message })
    }
  }

  // tslint:disable-next-line:no-any
  public async consume(queueName: string, cb: (cm: ConsumeMessage, channel: Channel) => any): Promise<void> {
    const channel: Channel = await this.getChannel()
    const { queue }: Replies.AssertQueue = await channel.assertQueue(queueName, { durable: true })

    channel.prefetch(1)
    channel.consume(queue, (cm: ConsumeMessage) => cb(cm, channel), { noAck: false })
  }

  public async ensureChannelIsAvailable(): Promise<void> {
    if (!this.channel || !this.connection) {
      return this.init()
    }
  }

  public async closeConnection(): Promise<void> {
    if (this.channel) {
      await this.channel.close()
    }
    if (this.connection) {
      await this.connection.close()
    }
  }

  private async init(): Promise<void> {
    if (!this.connection) {
      this.connection = await connect(this.configUrl)

      this.connection.once('close', () => this.connectionErrorCallback())
      this.connection.once('error', (error) => this.connectionErrorCallback(error))
    }

    if (!this.channel) {
      this.channel = await this.connection.createChannel()

      this.channel.once('error', (error) => this.channelErrorCallback(error))
    }
  }

  private handleMessageNotSendDrain(message: ISavedQueue): void {
    this.savedMessagesWaitingForDrainEvent.push(message)
    if (this.channel && this.channel.listeners('drain').length === 0) {
      this.channel.once('drain', this.channelDrainCallback)
    }
  }

  private connectionErrorCallback = (error?: Error): void => {
    if (this.channel && this.connection) {
      this.connection.removeAllListeners()
      this.channel.removeAllListeners()  
    }
    this.connection = null
    this.channel = null
    this.errorCallback(error)
  }
  
  private channelErrorCallback = (error?: Error): void => {
    if (this.channel) {
      this.channel.removeAllListeners()
    }
    this.channel = null
    this.errorCallback(error)
  }

  private channelDrainCallback = (): void => {
    if (!this.channel) {
      return
    }

    for (const { queueName, message } of this.savedMessagesWaitingForDrainEvent) {
      this.channel.sendToQueue(queueName, message)
    }
  }
}

export default RabbitMQMessageBroker
