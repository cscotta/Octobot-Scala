package com.urbanairship.octobot

import scala.collection.mutable
import java.lang.reflect.Method
import org.json.JSONObject

// This class is responsible for the actual invocation of a task.
// Given a task name and a JSON object to be passed onto it, the "execute"
// method looks up the task and method to invoke based on the name and calls it,
// then caches the method lookup.

object TaskExecutor {
  val taskCache = new mutable.HashMap[String, Method]
  val argClass = new JSONObject().getClass
  
  // Invokes a task identified by class name with a message.
  def execute(taskName: String, message: JSONObject) {

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
