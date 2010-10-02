package com.urbanairship.octobot

// Redis
import java.io.IOException
import redis.clients.jedis.{Jedis, JedisPubSub}

import java.io.{PrintWriter, StringWriter}
import org.json.{JSONObject, JSONTokener}
import org.apache.log4j.Logger


// This thread opens a streaming connection to a queue, which continually
// pushes messages to Octobot queue workers. The tasks contained within these
// messages are invoked, then acknowledged and removed from the queue.

class QueueConsumer(val queue: Queue) extends Runnable {
  val logger = Logger.getLogger("Queue Consumer")

  // Fire up the appropriate queue listener and begin invoking tasks!.
  override def run() {
      if (queue.queueType.equals("amqp")) {
          new AMQPConsumer().consume(queue)
      } else if (queue.queueType.equals("beanstalk")) {
          new BeanstalkConsumer().consume(queue)
      } else if (queue.queueType.equals("redis")) {
          consumeFromRedis()
      } else {
          logger.error("Invalid queue type specified: " + queue.queueType)
      }
  }

  def consumeFromRedis() {
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

object QueueConsumer {

  val logger = Logger.getLogger("Queue Consumer")
  val enableEmailErrors = Settings.getAsBoolean("Octobot", "email_enabled")
  
  // Invokes a task based on the name of the task passed in the message via
  // reflection, accounting for non-existent tasks and errors while running.
  def invokeTask(rawMessage: String) : Boolean = {
      var taskName = ""
      var message : JSONObject = null
      var retryCount = 0
      var retryTimes = 0

      val startedAt = System.nanoTime()
      var errorMessage: String = null
      var lastException: Throwable = null
      var executedSuccessfully = false

      while (!executedSuccessfully && retryCount < retryTimes + 1) {
          if (retryCount > 0)
              logger.info("Retrying task. Attempt " + retryCount + " of " + retryTimes)

          try {
            message = new JSONObject(new JSONTokener(rawMessage))
            taskName = message.get("task").asInstanceOf[String]

            if (message.has("retries")) {
              retryTimes = message.get("retries").asInstanceOf[Int]
            }
          } catch {
            case ex: Exception => {
              logger.error("Error: Invalid message received: " + rawMessage, ex)
              executedSuccessfully
            }
          }

          // Locate the task, then invoke it, supplying our message.
          // Cache methods after lookup to avoid unnecessary reflection lookups.
          try {
            TaskExecutor.execute(taskName, message)
            executedSuccessfully = true
          } catch {
            case ex: ClassNotFoundException => {
              lastException = ex
              errorMessage = "Error: Task requested not found: " + taskName
              logger.error(errorMessage)
            } case ex: NoClassDefFoundError => {
              lastException = ex
              errorMessage = "Error: Task requested not found: " + taskName
              logger.error(errorMessage, ex)
            } case ex: NoSuchMethodException => {
              lastException = ex
              errorMessage = "Error: Task requested does not have a static run method."
              logger.error(errorMessage, ex)
            } case ex: Throwable => {
              lastException = ex
              errorMessage = "An error occurred while running the task."
              logger.error(errorMessage, ex)
            }
          }

          if (!executedSuccessfully) retryCount += 1
      }

      // Deliver an e-mail error notification if enabled.
      if (enableEmailErrors && !executedSuccessfully) {
        val email = "Error running task: " + taskName + ".\n\n" +
          "Attempted executing " + retryCount.toString + " times as specified.\n\n" +
          "The original input was: \n\n" + rawMessage + "\n\n" +
          "Here's the error that resulted while running the task:\n\n" +
          stackToString(lastException)

        MailQueue.put(email)
      }

      val finishedAt = System.nanoTime()
      Metrics.update(taskName, finishedAt - startedAt, executedSuccessfully, retryCount)

      executedSuccessfully
  }

  // Converts a stacktrace from task invocation to a string for error logging.
  def stackToString(e: Throwable) : String = {
    if (e == null) "(Null)"

    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)

    e.printStackTrace(printWriter)
    stringWriter.toString
  }
}