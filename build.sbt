name := "weblog_sessionize"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.3"
val scoptVersion = "3.3.0"
val slf4jVersion = "1.7.22"
val typeSafeVesrion = "1.3.1"
val scalalogVersion = "3.7.2"
val pureConfigVersion = "0.10.2"
val hadoopVersion = "3.1.0"
val scalaTestVersion = "3.0.8"


resolvers += "bintray-spark-packages" at
  "https://dl.bintray.com/spark-packages/maven/"
resolvers += "Typesafe Simple Repository" at
  "http://repo.typesafe.com/typesafe/simple/maven-releases/"
resolvers += "MavenRepository" at
  "https://mvnrepository.com/"
// MAIN librar
libraryDependencies ++= Seq(
  "org.apache.spark"    %%  "spark-core"      % sparkVersion ,
  "org.apache.spark"    %% "spark-sql"        % sparkVersion ,
  "com.typesafe.scala-logging" %% "scala-logging" % scalalogVersion,
  "org.scalactic" %% "scalactic" % scalaTestVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
)

