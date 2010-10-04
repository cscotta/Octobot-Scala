package com.urbanairship.octobot

import java.util.Map
import com.twitter.json._
import java.lang.reflect.Method
import scala.collection.mutable
import scala.collection.JavaConversions._

// This class is responsible for the actual invocation of a task.
// Given a task name and a JSON object to be passed onto it, the "execute"
// method looks up the task and method to invoke based on the name and calls it,
// then caches the method lookup.

object TaskExecutor {
  val taskCache = new mutable.HashMap[String, Method]
  val argClass = Class.forName("java.util.Map")
  
  // Invokes a task identified by class name with a message.
  def execute(taskName: String, message: Map[String, Object]) {

    val method: Method = {
      taskCache.getOrElse(taskName, {
        val task = Class.forName(taskName)
        val meth = task.getMethod("run", argClass)
        taskCache.put(taskName, meth)
        meth
      })
    }

    method.invoke(null, message)
  }

}
