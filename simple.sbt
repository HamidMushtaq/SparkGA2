name := "DNASeqAnalyzer"

version := "1.0"

scalaVersion := "2.11.0"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

scalacOptions := Seq("-target:jvm-1.7")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"

libraryDependencies += "com.github.samtools" % "htsjdk" % "1.143"
