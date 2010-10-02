package com.urbanairship.octobot

import java.io.IOException
import org.json.JSONObject
import org.apache.log4j.Logger
import com.rabbitmq.client.{Channel, Connection, QueueingConsumer}

class AMQPConsumer extends Consumer {

  var channel: Channel = null
  var connection: Connection = null
  var consumer: QueueingConsumer = null
  val logger = Logger.getLogger("AMQP Consumer")

  // Attempts to register to receive streaming messages from RabbitMQ.
  // In the event that RabbitMQ is unavailable the call to getChannel()
  // will attempt to reconnect. If it fails, the loop simply repeats.
  def consume(queue: Queue) {
    
    var channel = getAMQPChannel(queue)
    
    while (true) {
      var task: QueueingConsumer.Delivery = null
      try { task = consumer.nextDelivery() }
      catch {
        case ex: Exception => {
          logger.error("Error in AMQP connection reconnecting.", ex)
          channel = getAMQPChannel(queue)
        }
      }

      // If we've got a message, fetch the body and invoke the task.
      // Then, send an acknowledgement back to RabbitMQ that we got it.
      if (task != null && task.getBody() != null) {
        QueueConsumer.invokeTask(new String(task.getBody()))
        try { channel.basicAck(task.getEnvelope().getDeliveryTag(), false) }
        catch {
          case ex: IOException => { logger.error("Error ack'ing message.", ex) }
        }
      }
    }
  }


  // Opens up a connection to RabbitMQ, retrying every five seconds
  // if the queue server is unavailable.
  def getAMQPChannel(queue: Queue) : Channel = {
    var attempts = 0
    var channel: Channel = null
    logger.info("Opening connection to AMQP " + queue.vhost + " "  + queue.queueName + "...")

    while (true) {
      attempts += 1
      logger.debug("Attempt #" + attempts)

      try {
        connection = new RabbitMQ(queue).getConnection()
        channel = connection.createChannel()
        consumer = new QueueingConsumer(channel)
        channel.exchangeDeclare(queue.queueName, "direct", true)
        channel.queueDeclare(queue.queueName, true, false, false, null)
        channel.queueBind(queue.queueName, queue.queueName, queue.queueName)
        channel.basicConsume(queue.queueName, false, consumer)
        logger.info("Connected to RabbitMQ")
        channel
      } catch {
        case ex: Exception => {
          logger.error("Cannot connect to AMQP. Retrying in 5 sec.", ex)
          Thread.sleep(1000 * 5)
        }
      }
    }

    channel
  }


}