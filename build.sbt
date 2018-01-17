name := "sparkPacktReading"

version := "0.1"

scalaVersion := "2.11.8"



libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.0.0",
  "org.apache.spark" %% "spark-sql" % "2.0.2",
  "org.apache.spark" %% "spark-mllib" % "2.0.2",
  "com.github.fommil.netlib" % "all" % "1.1.2"
)