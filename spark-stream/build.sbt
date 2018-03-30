name := "spark-stream"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.1"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}