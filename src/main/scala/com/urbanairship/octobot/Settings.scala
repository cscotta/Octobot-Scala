package com.urbanairship.octobot

import org.jvyaml.YAML
import java.util.HashMap
import java.io.FileReader
import org.apache.log4j.Logger


// This class is responsible for loading in configuration data for the
// application. By default, it searches for a YAML file located at
// /usr/local/octobot/octobot.yml, unless the JVM environment variable
// "-DconfigFile=/absolute/path.yml" is specified. These values are accessed
// as a standard map by calling Settings.get("Octobot", "queues").

// Implemented as an object to avoid reading the file in multiple times.
// Changes to application configuration require a restart to take effect.

object Settings {
  val logger = Logger.getLogger("Settings")
  var configuration:HashMap[String, HashMap[String, Any]] = null

  // Load the settings once on initialization, and hang onto them.
  var settingsFile = System.getProperty("configFile")
  if (settingsFile == null) settingsFile = "/usr/local/octobot/octobot.yml"

  // Load the configuration in from our Settings file, or warn
  // that the config could not be loaded and defaults will be used.
  try {
    configuration = YAML.load(new FileReader(settingsFile)).
        asInstanceOf[HashMap[String, HashMap[String, Any]]]
  } catch {
    case ex: Exception => {
      logger.warn("Warning: No valid config at " + settingsFile)
      logger.warn("Please create this file, or set the " +
          "-DconfigFile=/foo/bar/octobot.yml JVM variable to its location.")
      logger.warn("Continuing launch with internal defaults.")
    }
  }

  // Given a (Category, KeyName) pair, returns the value
  // of a config property from the app's configuration.
  def get(category: String, key: String) : String = {
    try {
      val configCategory = configuration.get(category)
      configCategory.get(key).toString
    } catch {
      case ex: NullPointerException =>
        logger.warn("Warning - unable to load " + category + " / " +
            key + " from configuration file.")
      null
    }
  }

  // Fetches a setting from YAML config and converts it to an integer.
  // No integer settings are autodetected, so that logic is not needed here.
  def getAsInt(category: String, key: String) : Int = {
    var result = 0
    var value : Any = null
    var configCategory : HashMap[String, Any] = null

    try {
      configCategory = configuration.get(category)
      value = configCategory.get(key)
      if (value != null) result = value.asInstanceOf[Long].intValue
    } catch {
      case ex: NullPointerException => {
        logger.warn("Warning - unable to load " + category + " / " +
            key + " from configuration file.")
      }
    }

    result
  }

  // Returns a safe integer value from config with a default fallback.
  def getIntFromYML(obj: Any, defaultValue : Int) : Int = {
    try {
      obj.toString.toInt
    } catch {
      case ex: Exception => {
        logger.info("Error reading settings.")
        defaultValue
      }
    }
  }

  // Fetches a setting from YAML config and converts it to a boolean.
  // No boolean settings are autodetected, so that logic is not needed here.
  def getAsBoolean(category: String, key: String) : Boolean = {
    try { get(category, key).toBoolean }
    catch { case ex: Exception => false }
  }
}
