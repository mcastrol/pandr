version := "0.1"
scalaVersion := "2.11.0"
val sparkVersion = "2.1.0"

lazy val PurchasesAndReturns = (project in file("."))
  .settings(
    name := "PurchasesAndReturns",
      libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion,
      libraryDependencies +="org.apache.spark" %% "spark-sql" % sparkVersion,
      libraryDependencies +="org.postgresql" % "postgresql" % "42.2.0"

)

