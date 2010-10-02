package com.urbanairship.octobot

import org.apache.log4j.Logger
import com.surftools.BeanstalkClientImpl.ClientImpl

// This class handles all interfacing with a Beanstalk in Octobot.
// It is responsible for connection initialization and management.

object Beanstalk {
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

