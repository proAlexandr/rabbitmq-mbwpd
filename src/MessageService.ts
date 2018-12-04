import { ConsumeMessage, Channel } from 'amqplib'
import RabbitMQMessageBroker from './messageBroker/RabbitMQMessageBroker'
import OfflineMessageBroker from './messageBroker/OfflineMessageBroker'
import MockMessageBroker from './messageBroker/MockMessageBroker'
import IMessageBroker from './messageBroker/IMessageBroker'

const MINUTES_TO_RECONNECT_TRY: number = 30


export interface IConsumerService {
  consumeMessage(message: ConsumeMessage, channel: Channel): void
}
class MessageService {
  private currentMessageBroker: IMessageBroker

  private rabbitMqUrl: string | null
  private serviceConsumeFunctions: {
    queueName: string;
    func(message: ConsumeMessage, channel: Channel): void;
  }[]
  private queueNames: Set<string>

  private reconnectTimeInterval: number | NodeJS.Timer | null
  private errorCallback: (error: Error) => void
  

  constructor(rabbitMqUrl: string | null, errorCallback: () => void = () => { return }) {
    if (!rabbitMqUrl) {
      this.rabbitMqUrl = null
      // tslint:disable-next-line:no-console
      console.info('Created MessageBroker with empty value for rabbitmq url. Sending message will be mocked for empty function.')
    } else {
      this.rabbitMqUrl = rabbitMqUrl
    }
    this.errorCallback = errorCallback

    this.serviceConsumeFunctions = []
    this.queueNames = new Set<string>()
    this.reconnectTimeInterval = null
  }

  public async setConsumer(queueName: string, service: IConsumerService): Promise<void> {
    this.serviceConsumeFunctions.push({ func: service.consumeMessage, queueName })
    await this.ensureMessageBroker()
    await this.bindServiceConsumerFunctionToCurrentMessageBroker()
  }

  public async sendMessage(queueName: string, messageToSend: Buffer): Promise<void> {
    try {
      this.queueNames.add(queueName)
      await this.ensureMessageBroker()
      await this.currentMessageBroker.sendToQueue(queueName, messageToSend)
    } catch (error) {
      this.handleError('Error occurred in MessageBroker.sendMessage', error)
      this.handleRabbitMQError(queueName, messageToSend)
    }
  }

  public async closeConnection(): Promise<void> {
    if (this.currentMessageBroker) {
      return this.currentMessageBroker.closeConnection()
    }
  }

  private async ensureMessageBroker(): Promise<void> {
    if (!this.currentMessageBroker) {
      await this.initMessageBroker()
    }

    if (RabbitMQMessageBroker.IS_RABBITMQ_MESSAGE_BROKER(this.currentMessageBroker)) {
      await this.currentMessageBroker.ensureChannelIsAvailable()
    }
  }

  private async initMessageBroker(): Promise<void> {
    if (this.isNeedToStartRabbitMQMessageBroker(this.rabbitMqUrl)) {
      try {
        const rabbitMQMessageBroker: RabbitMQMessageBroker = new RabbitMQMessageBroker(this.rabbitMqUrl, this.errorCallbackRabbitMQ)
        await rabbitMQMessageBroker.ensureChannelIsAvailable()
        await this.moveSavedQueue(rabbitMQMessageBroker)

        this.reInitRabbitMQClearInterval()
        this.currentMessageBroker = rabbitMQMessageBroker
      } catch (error) {
        this.handleError('Error occurred in MessageBroker.initMessageBroker', error)
        this.reInitRabbitMQSetInterval()
      }
    }
    if (this.isNeedToStartRabbitMQMessageBroker(this.rabbitMqUrl) && !this.currentMessageBroker) {
      this.currentMessageBroker = new OfflineMessageBroker()
    }

    if (!this.currentMessageBroker) {
      this.currentMessageBroker = new MockMessageBroker()
    }

    await this.bindServiceConsumerFunctionToCurrentMessageBroker()
  }

  private async bindServiceConsumerFunctionToCurrentMessageBroker(): Promise<void> {
    if (this.serviceConsumeFunctions.length > 0) {
      for (const { func, queueName } of this.serviceConsumeFunctions) {
        await this.currentMessageBroker.consume(queueName, func)
      }
    }
  }

  private async moveSavedQueue(rabbitMQMessageBroker: RabbitMQMessageBroker): Promise<void> {
    if (OfflineMessageBroker.IS_OFFLINE_MESSABE_BROKER(this.currentMessageBroker)) {
      await Promise.all([...this.queueNames.values()].map((queueName: string) =>
        this.currentMessageBroker.consume(queueName, ({ content }: ConsumeMessage) => {
          return rabbitMQMessageBroker.sendToQueue(queueName, content)
        })
      ))
    }
  }

  private errorCallbackRabbitMQ = (error?: Error): void => {
    if (error) {
      this.handleError('Error inside RabbitMQMessageBroker', error)
    }
    if (RabbitMQMessageBroker.IS_RABBITMQ_MESSAGE_BROKER(this.currentMessageBroker)) {
      this.currentMessageBroker.ensureChannelIsAvailable().catch((err: Error) => {
        console.error('Instant reinit of rabbitmq connection and channel failed', err)
        this.handleRabbitMQError()
        this.reInitRabbitMQSetInterval()
      })
    }
  }

  private handleError(message: string, error: Error): void {
    console.error(message, error)
    if (this.errorCallback) {
      this.errorCallback(error)
    }
  }

  private handleRabbitMQError(queueName: string | null = null, messageFailedToSend: Buffer | null = null): void {
    if (RabbitMQMessageBroker.IS_RABBITMQ_MESSAGE_BROKER(this.currentMessageBroker)) {
      this.currentMessageBroker = new OfflineMessageBroker()

      if (messageFailedToSend && queueName) {
        this.currentMessageBroker.sendToQueue(queueName, messageFailedToSend)
      }
    }
  }

  private reInitRabbitMQSetInterval(): void {
    if (!this.reconnectTimeInterval) {
      this.reconnectTimeInterval = setInterval(this.initMessageBroker.bind(this), MINUTES_TO_RECONNECT_TRY * 60 * 1000) as any
    }
  }

  private reInitRabbitMQClearInterval(): void {
    if (this.reconnectTimeInterval) {
      clearInterval(this.reconnectTimeInterval as any)
      this.reconnectTimeInterval = null
    }
  }

  private isNeedToStartRabbitMQMessageBroker(rabbitMqUrl: string | null): rabbitMqUrl is string {
    return this.rabbitMqUrl !== null
  }
}

export default MessageService
