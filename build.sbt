ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.16"

organization := "com.databricks.blog"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-sql" % "3.2.1" % "provided","com.snowflake" % "snowpark" % "1.6.1")

lazy val root = (project in file("."))
  .settings(
    name := "Scd2_Snowpark"
  )

