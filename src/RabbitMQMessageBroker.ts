import { Connection, Channel, connect, Replies, ConsumeMessage } from 'amqplib'
import { IMessageBroker } from './MessageBroker'

// all queues have { durable: true, noAck: false }. Dont forget to do `channel.ack(msg)` in consume callback
class RabbitMQMessageBroker implements IMessageBroker {
  private configUrl: string
  private connection: Connection | null
  private channel: Channel | null
  private handleErrorCallback: (error?: Error) => void

  constructor(configUrl: string, handleErrorCallback: () => void = (): void => { return }) {
    this.configUrl = configUrl
    this.handleErrorCallback = handleErrorCallback
  }

  public static IS_RABBITMQ_MESSABE_BROKER(messageBroker: IMessageBroker): messageBroker is RabbitMQMessageBroker {
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
      channel.on('drain', () => {
        channel.sendToQueue(queue, message)
      })
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

      this.connection.on('close', () => {
        this.connection = null
        this.handleErrorCallback()
      })
      this.connection.on('error', (error: Error) => {
        this.connection = null
        this.channel = null
        this.handleErrorCallback(error)
      })
    }

    if (!this.channel) {
      this.channel = await this.connection.createChannel()

      this.channel.on('error', (error: Error) => {
        this.channel = null
        this.handleErrorCallback(error)
      })

    }
  }
}

export default RabbitMQMessageBroker
