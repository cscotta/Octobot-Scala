package com.urbanairship.octobot

trait Consumer {
  def consume(queue: Queue)
}