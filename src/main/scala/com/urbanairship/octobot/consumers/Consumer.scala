package com.urbanairship.octobot.consumers

import com.urbanairship.octobot.Queue

trait Consumer {
  def consume(queue: Queue)
}