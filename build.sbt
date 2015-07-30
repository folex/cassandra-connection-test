name := """empty"""

version := "1.0"

scalaVersion := "2.10.5"

val akkaVersion = "2.4-SNAPSHOT"

resolvers ++= Seq(
  "sonatype" at "https://oss.sonatype.org/content/groups/public",
  "typesafe" at "https://repo.typesafe.com",
  "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/")

libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.7"
)

scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-Xfatal-warnings", "-encoding", "utf8")
