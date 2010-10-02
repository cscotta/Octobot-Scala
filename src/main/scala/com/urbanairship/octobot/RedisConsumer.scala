package com.urbanairship.octobot

import java.io.IOException
import org.apache.log4j.Logger
import redis.clients.jedis.{Jedis, JedisPubSub}

class RedisConsumer extends Consumer {

  val logger = Logger.getLogger("Redis Consumer")

  // Attempt to register to receive messages from Beanstalk and invoke tasks.
  def consume(queue: Queue) {
    logger.info("Connecting to Redis...")
    var jedis = new Jedis(queue.host, queue.port)

    try {
      jedis.connect()
    } catch {
      case ex: IOException => {
        logger.error("Unable to connect to Redis.", ex)
      }
    }

    logger.info("Connected to Redis.")

    jedis.subscribe(new JedisPubSub() {
	    override def onMessage(channel: String, message: String) {
		    QueueConsumer.invokeTask(message)
	    }

      override def onPMessage(string: String, string1: String, string2: String) {
          logger.info("onPMessage Triggered - Not implemented.")
      }

      override def onSubscribe(string: String, i: Int) {
        logger.info("onSubscribe called - Not implemented.")
      }

      override def onUnsubscribe(string: String, i: Int) {
        logger.info("onUnsubscribe Called - Not implemented.")
      }

      override def onPUnsubscribe(string: String, i: Int) {
        logger.info("onPUnsubscribe called - Not implemented.")
      }

      override def onPSubscribe(string: String, i: Int) {
        logger.info("onPSubscribe Triggered - Not implemented.")
      }
    }, queue.queueName)
  }


}