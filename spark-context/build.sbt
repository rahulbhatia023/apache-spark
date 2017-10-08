name := "spark-context"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}