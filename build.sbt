ThisBuild / version := "1.2.0"

ThisBuild / scalaVersion := "2.12.18"

lazy val PrintArgs = (project in file("."))
  .settings(
    name := "DemoDEVuHuyLoc",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
      "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
      "com.databricks" %% "dbutils-api" % "0.0.6" % "provided"
    )
  )


