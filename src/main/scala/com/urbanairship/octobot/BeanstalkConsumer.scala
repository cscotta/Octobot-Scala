package com.urbanairship.octobot

// Beanstalk Support
import com.surftools.BeanstalkClient.{Job, BeanstalkException}
import com.surftools.BeanstalkClientImpl.ClientImpl
import org.apache.log4j.Logger

class BeanstalkConsumer extends Consumer {

  val logger = Logger.getLogger("Beanstalk Consumer")

  // Attempt to register to receive messages from Beanstalk and invoke tasks.
  def consume(queue: Queue) {
    var beanstalkClient = Beanstalk.getBeanstalkChannel(queue.host, queue.port, queue.queueName)
    logger.info("Connected to Beanstalk waiting for jobs.")

    while (true) {
      var job: Job = null
      try { job = beanstalkClient.reserve(1) }
      catch {
        case ex: BeanstalkException => {
          logger.error("Beanstalk connection error.", ex)
          beanstalkClient = Beanstalk.getBeanstalkChannel(queue.host,
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
            beanstalkClient = Beanstalk.getBeanstalkChannel(queue.host,
                queue.port, queue.queueName)
          }
        }
      }
    }
  }


}