name := "spark-sql"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}