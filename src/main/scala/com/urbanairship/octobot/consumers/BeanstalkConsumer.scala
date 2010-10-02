package com.urbanairship.octobot.consumers

// Beanstalk Support
import org.apache.log4j.Logger
import com.surftools.BeanstalkClientImpl.ClientImpl
import com.surftools.BeanstalkClient.{Job, BeanstalkException}
import com.urbanairship.octobot.{Queue, QueueConsumer}

class BeanstalkConsumer extends Consumer {

  val logger = Logger.getLogger("Beanstalk Consumer")

  // Attempt to register to receive messages from Beanstalk and invoke tasks.
  def consume(queue: Queue) {
    var beanstalkClient = BeanstalkConsumer.getBeanstalkChannel(queue.host, queue.port, queue.queueName)
    logger.info("Connected to Beanstalk waiting for jobs.")

    while (true) {
      var job: Job = null
      try { job = beanstalkClient.reserve(1) }
      catch {
        case ex: BeanstalkException => {
          logger.error("Beanstalk connection error.", ex)
          beanstalkClient = BeanstalkConsumer.getBeanstalkChannel(queue.host,
                  queue.port, queue.queueName)
        }
      }

      if (job != null) {
        val message = new String(job.getData())

        try {
          QueueConsumer.invokeTask(message)
        } catch {
          case ex: Exception => logger.error("Error handling message.", ex)
        }

        try {
          beanstalkClient.delete(job.getJobId())
        } catch {
          case ex: BeanstalkException => {
            logger.error("Error sending message receipt.", ex)
            beanstalkClient = BeanstalkConsumer.getBeanstalkChannel(queue.host,
                queue.port, queue.queueName)
          }
        }
      }
    }
  }
}


object BeanstalkConsumer {
  val logger = Logger.getLogger("Beanstalk")

  def getBeanstalkChannel(host: String, port: Int, tube: String, attempts : Int) : ClientImpl = {
    logger.info("Opening connection to Beanstalk tube: '" + tube + "'. Attempt #" + attempts)

    val beanstalkClient: ClientImpl = {
      try {
          val client = new ClientImpl(host, port)
          client.useTube(tube)
          client.watch(tube)
          client
      } catch {
        case ex: Exception => {
          logger.error("Unable to connect to Beanstalk. Retrying in 5 seconds", ex)
          Thread.sleep(1000 * 5)
          getBeanstalkChannel(host, port, tube, attempts + 1)
        }
      }
    }

    beanstalkClient
  }

  def getBeanstalkChannel(host: String, port: Int, tube: String) : ClientImpl = {
    getBeanstalkChannel(host, port, tube, 1)
  }
}
