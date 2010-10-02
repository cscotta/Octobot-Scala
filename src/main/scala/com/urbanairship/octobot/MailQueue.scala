package com.urbanairship.octobot

import java.util.Properties
import javax.mail.{Message, Session, Transport, MessagingException, PasswordAuthentication}
import javax.mail.internet.{InternetAddress, MimeMessage}
import java.util.concurrent.ArrayBlockingQueue
import org.apache.log4j.Logger

// This class provides an internal queue allowing us to asynchronously
// send email notifications rather than processing them in main app loop.

object MailQueue extends Runnable {
  val logger = Logger.getLogger("Email Queue")
  val from = Settings.get("Octobot", "email_from")
  val recipient = Settings.get("Octobot", "email_to")
  val server = Settings.get("Octobot", "email_server")
  val username = Settings.get("Octobot", "email_username")
  val password = Settings.get("Octobot", "email_password")
  val port = Settings.getAsInt("Octobot", "email_port")
  val useSSL = Settings.getAsBoolean("Octobot", "email_ssl")
  val useAuth = Settings.getAsBoolean("Octobot", "email_auth")

  // This internal queue is backed by an ArrayBlockingQueue. By specifying the
  // number of messages to be held here before the queue blocks (below), we
  // provide ourselves a safety threshold in terms of how many messages could
  // be backed up before we force the delivery of all current waiting messages.
  val messages = new ArrayBlockingQueue[String](100)

  def put(message: String) {
    messages.put(message)
  }

  def size() : Int = {
    messages.size()
  }

  // As this thread runs, it consumes messages from the internal queue and
  // delivers each to the recipients configured in the YML file.
  override def run() {
    if (!validSettings()) {
      logger.error("Email settings invalid check your configuration.")
      return
    }

    while (true)
      deliverMessage(messages.take())
  }

  // Delivers email error notificiations.
  def deliverMessage(message: String) {
    logger.info("Sending error notification to: " + recipient)

    try {
      val email = prepareEmail()

      email.setFrom(new InternetAddress(from))
      email.addRecipient(Message.RecipientType.TO, new InternetAddress(recipient))
      email.setSubject("Task Error Notification")
      email.setText(message)

      // Send message
      Transport.send(email)
      logger.info("Sent error e-mail to " + recipient + ". "
          + "Message: \n\n" + message)

    } catch {
      case ex: MessagingException =>
        logger.error("Error delivering error notification.", ex)
    }
  }


  // Prepares a sendable email object based on Octobot's SMTP, SSL, and
  // Authentication setup specified in its YML config file.
  def prepareEmail() : MimeMessage = {
    val properties = System.getProperties()
    properties.setProperty("mail.smtp.host", server)
    properties.put("mail.smtp.auth", "true")

    // Configure SSL.
    if (useSSL) {
      properties.put("mail.smtp.socketFactory.port", port.asInstanceOf[AnyRef])
      properties.put("mail.smtp.starttls.enable","true")
      properties.put("mail.smtp.socketFactory.fallback", "false")
      properties.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory")
    }

    // Configure authentication.
    val session = {
      if (useAuth) {
        properties.setProperty("mail.smtp.submitter", username)
        val authenticator = new Authenticator(username, password)
        Session.getInstance(properties, authenticator)
      } else {
        Session.getDefaultInstance(properties)
      }
    }

    new MimeMessage(session)
  }


  // Provides an SMTP authenticator for messages sent with auth.
  class Authenticator(val user: String, val pass: String, var authenticator: Authenticator)
    extends javax.mail.Authenticator {

    def this(user: String, pass: String) {
      this(user, pass, new PasswordAuthentication(username, password).asInstanceOf[Authenticator])
    }
  }

  // Validates Octobot's e-mail settings, ensuring that everything's here
  // before spawning the queue and preparing to batch out messages.
  def validSettings() : Boolean = {
    var valid = true

    // Validate base settings.
    List(from, recipient, server, port).foreach { setting =>
      if (setting == null) valid = false
    }

    // Validate authentication.
    if (useAuth && (username == null || password == null))
      valid = false

    valid
  }
}
