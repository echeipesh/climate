import sbt._
import sbt.Keys._

object ClimateBuild extends Build {
  val scalaOptions = Seq(
        "-deprecation",
        "-unchecked",
        "-Yclosure-elim",
        "-Yinline-warnings",
        "-optimize",
        "-language:implicitConversions",
        "-language:postfixOps",
        "-language:existentials",
        "-feature"
  )

  val key = AttributeKey[Boolean]("javaOptionsPatched")

  lazy val root = 
    Project("root", file(".")).settings(
      organization := "com.azavea.geotrellis",
      name := "climate",
      version := "0.1.0-SNAPSHOT",
      scalaVersion := "2.10.3",
     
      scalacOptions ++= scalaOptions,
      scalacOptions in Compile in doc ++= Seq("-diagrams", "-implicits"),
      parallelExecution := false,

      fork in run := true,

      mainClass := Some("climate.Main"),

      javaOptions in (Compile,run) ++= Seq("-Xmx2G"),
      javaOptions += "-Djava.library.path=/usr/local/lib",

      libraryDependencies ++= Seq(
        "com.azavea.geotrellis" %% "geotrellis-spark" % "0.10.0-SNAPSHOT",
        "com.azavea.geotrellis" %% "geotrellis-gdal" % "0.10.0-SNAPSHOT",
        "joda-time" % "joda-time" % "2.3",
        "org.joda" % "joda-convert" % "1.6",
        "com.typesafe" % "config" % "1.0.2",
        "org.spire-math" %% "spire" % "0.7.4",
        "org.scalatest" %% "scalatest" % "2.1.5" % "test",
        "com.google.guava" % "guava" % "14.0.1",
        "org.apache.spark" %% "spark-core" % "0.9.0-incubating" excludeAll (
          ExclusionRule(organization = "org.apache.hadoop")),
        "org.apache.hadoop" % "hadoop-client" % "1.2.1" excludeAll (
	  ExclusionRule(organization = "hsqldb"))
//        "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.3.0",
//        "com.quantifind" %% "sumac" % "0.2.3"
      ),

      resolvers ++= Seq(
        "Cloudera Repo" at "https://repository.cloudera.com/artifactory/cloudera-repos",
        "opengeo" at "http://repo.opengeo.org/"
      ),
    
      licenses := Seq("MIT License" -> url("http://www.opensource.org/licenses/mit-license.html"))
    )
}
