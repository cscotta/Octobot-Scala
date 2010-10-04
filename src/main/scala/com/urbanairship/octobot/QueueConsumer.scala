package com.urbanairship.octobot

import java.io.{PrintWriter, StringWriter}
import com.twitter.json._
import org.apache.log4j.Logger
import com.urbanairship.octobot.consumers._
import scala.collection.JavaConversions._


// This thread opens a streaming connection to a queue, which continually
// pushes messages to Octobot queue workers. The tasks contained within these
// messages are invoked, then acknowledged and removed from the queue.

class QueueConsumer(val queue: Queue) extends Runnable {
  val logger = Logger.getLogger("Queue Consumer")

  // Fire up the appropriate queue listener and begin invoking tasks!.
  override def run() {
    queue.queueType match {
      case "amqp"       => new AMQPConsumer().consume(queue)
      case "beanstalk"  => new BeanstalkConsumer().consume(queue)
      case "redis"      => new RedisConsumer().consume(queue)
      case _            => logger.error("Invalid queue type: " + queue.queueType)
    }
  }
}


object QueueConsumer {

  val logger = Logger.getLogger("Queue Consumer")
  val enableEmailErrors = Settings.getAsBoolean("Octobot", "email_enabled")
  
  // Invokes a task based on the name of the task passed in the message via
  // reflection, accounting for non-existent tasks and errors while running.
  def invokeTask(rawMessage: String) : Boolean = {
    var taskName = ""
    var message: Map[String, Object] = null
    var retryCount = 0
    var retryTimes = 0

    val startedAt = System.nanoTime()
    var errorMessage: String = null
    var lastException: Throwable = null
    var executedSuccessfully = false

    // Attempt to execute a task until it succeeds, or exceeds its retry count.
    while (!executedSuccessfully && retryCount < retryTimes + 1) {
      if (retryCount > 0)
        logger.info("Retrying task. Attempt " + retryCount + " of " + retryTimes)

      try {
        message = Json.parse(rawMessage).asInstanceOf[Map[String, Object]]
        taskName = message.get("task").get.toString

        if (message.contains("retries"))
          retryTimes = message.get("retries").get.asInstanceOf[Int]

      } catch {
        case ex: Exception => {
          logger.error("Error: Invalid message received: " + rawMessage, ex)
          return executedSuccessfully
        }
      }

      // Invoke the task with the message received - very carefully.
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
    if (enableEmailErrors && !executedSuccessfully)
      sendEmail(taskName, retryCount, rawMessage, lastException)

    // Update our task execution metrics, and finish up.
    Metrics.update(taskName, System.nanoTime() - startedAt, executedSuccessfully, retryCount)
    executedSuccessfully
  }

  // Sends an e-mail error notification on task failure if enabled.
  def sendEmail(taskName: String, retries: Int, message: String, exception: Throwable) {
    val email = "Error running task: " + taskName + ".\n\n" +
      "Attempted executing " + retries + " times as specified.\n\n" +
      "The original input was: \n\n" + message + "\n\n" +
      "Here's the error that resulted while running the task:\n\n" +
      stackToString(exception)

    MailQueue.put(email)
  }

  // Converts a stacktrace from task invocation to a string for error logging.
  def stackToString(e: Throwable) : String = {
    if (e == null) return "(Null)"

    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)

    e.printStackTrace(printWriter)
    stringWriter.toString
  }
}