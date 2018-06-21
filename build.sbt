
version := "1.0"

scalaVersion := "2.12.6"

libraryDependencies ++= Seq(
  "junit" % "junit" % "4.12" % Test,
  "com.google.cloud" % "google-cloud-bigquery" % "1.29.0",
  "com.google.cloud" % "google-cloud-storage" % "1.29.0",
  "com.typesafe.akka" %% "akka-stream"                  % "2.5.12",
  "com.typesafe.akka" %% "akka-actor"                   % "2.5.12",
  "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "0.19",
)

enablePlugins(JavaAppPackaging)