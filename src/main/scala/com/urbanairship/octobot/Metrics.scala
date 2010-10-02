package com.urbanairship.octobot

import scala.collection.mutable
import java.util.{ArrayList, LinkedList}

// This class is responsible for maintaining task execution metrics
// such as successes, failures, retries, and execution times.

object Metrics {

  // Keep track of all tasks we've seen executed, their
  // execution times, along with success, failure, and retry counts.
  val instrumentedTasks = new ArrayList[String]
  val executionTimes = new mutable.HashMap[String, LinkedList[Long]]
  val taskSuccesses = new mutable.HashMap[String, Int]
  val taskFailures = new mutable.HashMap[String, Int]
  val taskRetries = new mutable.HashMap[String, Int]
  val metricsLock = new Object

  // Updates internal metrics following task execution.
  def update(task: String, time: Long, status: Boolean, retries: Int) {
    metricsLock.synchronized {
      if (!instrumentedTasks.contains(task))
        instrumentedTasks.add(task)

      updateExecutionTimes(task, time)
      updateTaskRetries(task, retries)
      updateTaskResults(task, status)
    }
  }

  // Update the list of execution times, keeping the last 10,000 per task.
  def updateExecutionTimes(task: String, time: Long) {
    val timeList = executionTimes.getOrElse(task, new LinkedList[Long])
    if (timeList.size() == 10000) timeList.removeLast()

    timeList.addFirst(time)
    executionTimes.put(task, timeList)
  }

  // Update the number of times this task has been retried.
  def updateTaskRetries(task: String, retries: Int) {
    val prevRetries = taskRetries.getOrElse(task, 0)
    taskRetries.put(task, prevRetries + retries)
  }

  // Update the number of times this task has succeeded or failed.
  def updateTaskResults(task: String, status: Boolean) {
    if (status == true) {
      val successes = taskSuccesses.getOrElse(task, 0)
      taskSuccesses.put(task, successes + 1)
    } else {
      val failures = taskFailures.getOrElse(task, 0)
      taskFailures.put(task, failures + 1)
    }
  }
}

