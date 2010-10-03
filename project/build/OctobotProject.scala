import sbt._

class OctobotProject(info: ProjectInfo) extends DefaultWebProject(info) {
  // repositories
  val scalatoolsSnapshot = "Scala Tools Snapshot" at "http://scala-tools.org/repo-snapshots/"
  val scalatoolsRelease = "Scala Tools Release" at "http://scala-tools.org/repo-releases/"
  val ibiblioRepository = "ibiblio" at "http://mirrors.ibiblio.org/pub/mirrors/maven2/"
  val mavenRepository = "maven" at "http://repo1.maven.org/maven2/"
  val javaNetRepo = "Java Dot Net" at "http://download.java.net/maven/2/"

  // dependencies
  val specs = "org.scala-tools.testing" % "specs_2.8.0" % "1.6.5"
  val log4j = "log4j" % "log4j" % "1.2.16"
  val amqpClient = "com.rabbitmq" % "amqp-client" % "2.1.0"
  val mail = "javax.mail" % "mail" % "1.4.3"
  val jvyaml = "net.java.dev" % "jvyaml" % "0.2.1"
  // beanstalkclient in lib/
  // scala-json in lib/
  // jedis  in lib/
}
