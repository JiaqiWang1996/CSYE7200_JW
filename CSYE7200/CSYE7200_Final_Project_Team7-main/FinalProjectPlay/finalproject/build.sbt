name := """FinalProject"""
organization := "com.neu.edu.CSYE7200"

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.12.13"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.7" % "test"

libraryDependencies += guice

libraryDependencies += "org.scalatestplus.play" %% "scalatestplus-play" % "5.0.0" % Test

// Adds additional packages into Twirl
//TwirlKeys.templateImports += "com.neu.edu.CSYE7200.controllers._"

// Adds additional packages into conf/routes
// play.sbt.routes.RoutesKeys.routesImport += "com.neu.edu.CSYE7200.binders._"
