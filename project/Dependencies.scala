import sbt._

object Dependencies {
  // versions
  lazy val sparkVersion = "3.3.2"

  // testing
  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.7" % "test,it"

  // arc
  val arc = "ai.tripl" %% "arc" % "3.13.2" % "provided"

  // spark
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  val sparkHive = "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
  val sparkAvro = "org.apache.spark" %% "spark-avro" % sparkVersion % "provided"

  val sparkSQLKafka = "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion

  val mysql = "mysql" % "mysql-connector-java" % "8.0.22" % "it"
  val mongo = "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.2" % "it"
  val bson = "org.mongodb" % "bson" % "4.1.1"
  val postgresJDBC = "org.postgresql" % "postgresql" % "42.2.8" % "it"

  // Project
  val etlDeps = Seq(
    scalaTest,

    arc,

    sparkSql,
    sparkHive,
    sparkAvro,

    sparkSQLKafka,

    mysql,
    mongo,
    bson,
    postgresJDBC,
  )
}