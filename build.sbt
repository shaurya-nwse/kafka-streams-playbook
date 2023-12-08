ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

val baseName     = "hackweek-ks"
val slf4jVersion = "2.0.0-alpha5"
val kafkaVersion = "3.0.0"

lazy val ks = project
  .in(file("ks-worker"))
  .settings(
    name := s"$baseName-worker",
    libraryDependencies ++= Seq(
      "org.apache.kafka"     % "kafka-clients"            % kafkaVersion,
      "org.apache.kafka"     % "kafka-streams"            % kafkaVersion,
      "org.apache.kafka"     %% "kafka-streams-scala"     % kafkaVersion,
      "io.circe"             %% "circe-core"              % "0.14.1",
      "io.circe"             %% "circe-generic"           % "0.14.1",
      "io.circe"             %% "circe-parser"            % "0.14.1",
      "org.slf4j"            % "slf4j-api"                % slf4jVersion,
      "org.slf4j"            % "slf4j-log4j12"            % slf4jVersion,
      "org.apache.avro"      % "avro"                     % "1.10.2",
      "io.confluent"         % "kafka-streams-avro-serde" % "7.0.1",
      "com.typesafe.play"    %% "play-json"               % "2.7.4",
      "com.google.code.gson" % "gson"                     % "2.8.5",
      // Simple REST Server
      "io.javalin" % "javalin" % "4.3.0",
      // Simple HTTP client
      "com.squareup.okhttp3" % "okhttp" % "4.9.1",
      // Config
      "com.typesafe" % "config" % "1.4.1"
    ),
    resolvers += "confluent" at "https://packages.confluent.io/maven/",
    Compile / sourceGenerators += (Compile / avroScalaGenerateSpecific).taskValue
  )
