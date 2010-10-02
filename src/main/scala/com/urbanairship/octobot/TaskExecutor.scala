package com.urbanairship.octobot

import scala.collection.mutable
import java.lang.reflect.Method
import org.json.JSONObject

object TaskExecutor {
  val taskCache = new mutable.HashMap[String, Method]
  val argClass = new JSONObject().getClass
  
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
