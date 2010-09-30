package com.urbanairship.octobot

import scala.collection.mutable
import java.lang.reflect.Method
import org.json.JSONObject

object TaskExecutor {
  val taskCache = new mutable.HashMap[String, Method]

  def execute(taskName: String, message: JSONObject) {
    var method: Method = null

    if (taskCache.contains(taskName)) {
      // TODO refactor as getOrElse
      method = taskCache.get(taskName).get
    } else {
      val task = Class.forName(taskName)
      val klass = new JSONObject().getClass

      method = task.getMethod("run", klass)
      taskCache.put(taskName, method)
    }

    method.invoke(null, message)
  }
}
