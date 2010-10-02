package com.urbanairship.octobot

import java.io.IOException
import org.apache.log4j.Logger
import com.rabbitmq.client.{Channel, Connection, ConnectionFactory}

// This class handles all interfacing with AMQP / RabbitMQ in Octobot.
// It provides basic connection management and returns task channels
// for placing messages into a remote queue.

class RabbitMQ(val host: String, val port: Int, val username: String,
               val password: String, val vhost: String) {

}

