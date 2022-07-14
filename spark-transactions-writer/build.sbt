name := "spark-transactions-writer"
version := "0.1"
scalaVersion := "2.11.12"


//Scala Libraries
libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.12"

//Spark Libraries
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.0" % "provided"

//Util Libraries
libraryDependencies += "com.typesafe" % "config" % "1.3.2"
libraryDependencies += "commons-io" % "commons-io" % "2.6"

//Mysql Connector
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.29"



