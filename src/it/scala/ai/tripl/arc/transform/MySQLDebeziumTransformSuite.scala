package ai.tripl.arc

import java.net.URI
import java.sql.DriverManager
import java.text.DecimalFormat
import java.util.UUID

import scala.collection.JavaConverters._
import scala.util.Random

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.StreamingQueryListener._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util.log.LoggerFactory
import ai.tripl.arc.udf.UDF

import ai.tripl.arc.util._

class MySQLDebeziumTransformSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _

  val inputView = "inputView"
  val outputView = "outputView"
  val schema = "schema"
  val checkpointLocation = "/tmp/debezium"

  val mysqlURL = "jdbc:mysql://mysql:3306/inventory?user=root&password=debezium&allowMultiQueries=true"
  val connectURI = s"http://connect:8083/connectors/"
  val connectorName = "inventory-connector-mysql"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "4040")
                  .config("spark.checkpoint.compress", "true")
                  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                  .config("spark.sql.streaming.checkpointLocation", checkpointLocation)
                  .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
                  .appName("Arc Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    implicit val logger = TestUtils.getLogger()

    val drivers = DriverManager.getDrivers

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark

    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }
      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }
      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println(s"numRowsTotal: ${queryProgress.progress.stateOperators(0).numRowsTotal} inputRowsPerSecond: ${queryProgress.progress.inputRowsPerSecond.round} processedRowsPerSecond: ${queryProgress.progress.processedRowsPerSecond.round}")
      }
    })

    FileUtils.deleteQuietly(new java.io.File(checkpointLocation))
  }

  after {
    FileUtils.deleteQuietly(new java.io.File(checkpointLocation))
    session.stop
  }

  def makeConnectorConfig(tableName: String, key: String): String = {
    s"""{
    |  "name": "${connectorName}",
    |  "config": {
    |    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    |    "tasks.max": "1",
    |    "database.hostname": "mysql",
    |    "database.port": "3306",
    |    "database.user": "debezium",
    |    "database.password": "dbz",
    |    "database.server.id": "184054",
    |    "database.server.name": "dbserver2",
    |    "database.whitelist": "inventory",
    |    "database.history.kafka.bootstrap.servers": "kafka:9092",
    |    "database.history.kafka.topic": "schema-changes.inventory",
    |    "message.key.columns": "${tableName}:${key}",
    |    "decimal.handling.mode": "string"
    |  }
    |}""".stripMargin
  }

  def makeTransaction(statements: Seq[String]): String = {
    s"""START TRANSACTION;
    |${statements.mkString("\n")}
    |COMMIT;
    """.stripMargin
  }

  def makeTransactions(customerInitial: Dataset[ai.tripl.arc.util.Customer], customerUpdates: Seq[ai.tripl.arc.util.Customer], tableName: String, seed: Int, limit: Int = Int.MaxValue): (Seq[String], Int, Int, Int) = {

    val random = new Random(seed)

    val customerUpdatesShuffle = random.shuffle(customerUpdates).take(limit)

    val existingIds = customerInitial.collect.map { customer => customer.c_custkey }.toSeq

    var transactions = Seq[String]()
    var i = 0
    var updates = 0
    var inserts = 0
    var deletes = 0

    while (i < customerUpdatesShuffle.length) {
      val len = (random.nextGaussian.abs * 5).ceil.toInt
      val transaction = makeTransaction(
      customerUpdatesShuffle.drop(i).take(len).flatMap { customer =>
        random.nextInt(4) match {
          // full update
          case 0 => {
            updates += 1
            Seq(s"UPDATE ${tableName} SET c_name='${customer.c_name}', c_address='${customer.c_address}', c_nationkey=${customer.c_nationkey}, c_phone='${customer.c_phone}', c_acctbal=${customer.c_acctbal}, c_mktsegment='${customer.c_mktsegment}', c_comment='${customer.c_comment}' WHERE c_custkey=${customer.c_custkey};")
          }
          // delete
          case 1 => {
            if (existingIds.contains(customer.c_custkey)) {
              deletes += 1
              Seq(s"DELETE FROM ${tableName} WHERE c_custkey=${customer.c_custkey};")
            } else {
              inserts += 1
              Seq(s"INSERT INTO ${tableName} (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) VALUES (${customer.c_custkey}, '${customer.c_name}', '${customer.c_address}', ${customer.c_nationkey}, '${customer.c_phone}', ${customer.c_acctbal}, '${customer.c_mktsegment}', '${customer.c_comment}');")
            }
          }
          // delete then insert
          case 2 => {
            if (existingIds.contains(customer.c_custkey)) {
              deletes += 1
              inserts += 1
              Seq(
                s"DELETE FROM ${tableName} WHERE c_custkey=${customer.c_custkey};",
                s"INSERT INTO ${tableName} (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) VALUES (${customer.c_custkey}, '${customer.c_name}', '${customer.c_address}', ${customer.c_nationkey}, '${customer.c_phone}', ${customer.c_acctbal}, '${customer.c_mktsegment}', '${customer.c_comment}');",
              )
            } else {
              inserts += 1
              Seq(s"INSERT INTO ${tableName} (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) VALUES (${customer.c_custkey}, '${customer.c_name}', '${customer.c_address}', ${customer.c_nationkey}, '${customer.c_phone}', ${customer.c_acctbal}, '${customer.c_mktsegment}', '${customer.c_comment}');")
            }
          }
          // delete then insert + update (just swap first/last name)
          case 3 => {
            if (existingIds.contains(customer.c_custkey)) {
              deletes += 1
              inserts += 1
              updates += 1
              Seq(
                s"DELETE FROM ${tableName} WHERE c_custkey=${customer.c_custkey};",
                s"INSERT INTO ${tableName} (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) VALUES (${customer.c_custkey}, '${customer.c_name}', '${customer.c_address}', ${customer.c_nationkey}, '${customer.c_phone}', ${customer.c_acctbal}, '${customer.c_mktsegment}', '${customer.c_comment}');",
                s"UPDATE ${tableName} SET c_name='${customer.c_name}', c_address='${customer.c_address}', c_nationkey=${customer.c_nationkey}, c_phone='${customer.c_phone}', c_acctbal=${customer.c_acctbal}, c_mktsegment='${customer.c_mktsegment}', c_comment='${customer.c_comment}' WHERE c_custkey=${customer.c_custkey};",
              )
            } else {
              inserts += 1
              updates += 1
              Seq(
                s"INSERT INTO ${tableName} (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) VALUES (${customer.c_custkey}, '${customer.c_name}', '${customer.c_address}', ${customer.c_nationkey}, '${customer.c_phone}', ${customer.c_acctbal}, '${customer.c_mktsegment}', '${customer.c_comment}');",
                s"UPDATE ${tableName} SET c_name='${customer.c_name}', c_address='${customer.c_address}', c_nationkey=${customer.c_nationkey}, c_phone='${customer.c_phone}', c_acctbal=${customer.c_acctbal}, c_mktsegment='${customer.c_mktsegment}', c_comment='${customer.c_comment}' WHERE c_custkey=${customer.c_custkey};",
              )
            }
          }
        }
      })

      transactions = transactions :+ transaction
      i += len
    }

    (transactions, updates, inserts, deletes)
  }

  test("MySQLDebeziumTransform") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming = true)

    val (customerInitial, customerUpdates) = TestHelpers.getTestData(50000)
    val customersMetadata = MetadataUtils.createMetadataDataframe(customerInitial.toDF)
    customersMetadata.persist
    customersMetadata.createOrReplaceTempView(schema)

    println()
    for (seed <- 0 to 0) {
      for (strict <- Seq(true, false)) {
        val tableName = s"customers_${UUID.randomUUID.toString.replaceAll("-","")}"
        println(s"mysql ${if (strict) "strict" else "not-strict"} seed: ${seed} target: ${tableName}")

        ai.tripl.arc.execute.JDBCExecuteStage.execute(
          ai.tripl.arc.execute.JDBCExecuteStage(
            plugin=new ai.tripl.arc.execute.JDBCExecute,
            id=None,
            name="JDBCExecute",
            description=None,
            inputURI=new URI(mysqlURL),
            jdbcURL=mysqlURL,
            sql=makeTransaction(Seq(s"CREATE TABLE ${tableName} (c_custkey INTEGER PRIMARY KEY NOT NULL, c_name VARCHAR(25) NOT NULL, c_address VARCHAR(40) NOT NULL, c_nationkey INTEGER NOT NULL, c_phone VARCHAR(15) NOT NULL, c_acctbal DECIMAL(20,2) NOT NULL, c_mktsegment VARCHAR(10) NOT NULL, c_comment VARCHAR(117) NOT NULL);")),
            params=Map.empty,
            sqlParams=Map.empty
          )
        )
        customerInitial.write.mode("append").jdbc(mysqlURL, s"inventory.${tableName}", new java.util.Properties)

        // make transactions
        val (transactions, update, insert, delete) = makeTransactions(customerInitial, customerUpdates, tableName, seed)

        TestHelpers.registerConnector(connectURI, makeConnectorConfig(s"inventory.${tableName}", "c_custkey"))

        val readStream = spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "kafka:9092")
          .option("subscribe", s"dbserver2.inventory.${tableName}")
          .option("startingOffsets", "earliest")
          .load
        readStream.createOrReplaceTempView(inputView)

        transform.DebeziumTransformStage.execute(
          transform.DebeziumTransformStage(
            plugin=new transform.DebeziumTransform,
            id=None,
            name="DebeziumTransform",
            description=None,
            inputView=inputView,
            outputView=outputView,
            schema=Left(schema),
            strict=strict
          )
        )

        val writeStream = spark.table(outputView)
          .writeStream
          .outputMode("complete")
          .queryName(tableName)
          .format("memory")
          .start

        try {
          // wait for query to start
          val start = System.currentTimeMillis()
          while (writeStream.lastProgress == null || (writeStream.lastProgress != null && writeStream.lastProgress.numInputRows == 0)) {
            if (System.currentTimeMillis() > start + 180000) throw new Exception("Timeout without messages arriving")
            println("Waiting for query progress...")
            Thread.sleep(1000)
          }

          // while running perform PARALLEL insert/update/delete transactions
          // this will block the main thread but we want to process all updates before triggering awaitTermination
          var last = System.currentTimeMillis()
          var i = 0
          var deadlocks = 0
          transactions.par.foreach { sql =>
            if (System.currentTimeMillis() > last+1000) {
              last = System.currentTimeMillis()
              println(s"${i} transactions/sec")
              i = 0
            }
            i += 1
            try {
              ai.tripl.arc.execute.JDBCExecuteStage.execute(
                ai.tripl.arc.execute.JDBCExecuteStage(
                  plugin=new ai.tripl.arc.execute.JDBCExecute,
                  id=None,
                  name="JDBCExecute",
                  description=None,
                  inputURI=new URI(mysqlURL),
                  jdbcURL=mysqlURL,
                  sql=sql,
                  params=Map.empty,
                  sqlParams=Map.empty
                )
              )
            } catch {
              // not nice but sometimes get deadlocks and we can just ignore them
              case e: Exception if e.getMessage.contains("Deadlock") => deadlocks += 1
            }
          }

          if (deadlocks > 10) throw new Exception(s"too many (${deadlocks}) transactions ignored due to database deadlock")

          writeStream.processAllAvailable
          writeStream.stop

          // validate results
          val expected = extract.JDBCExtractStage.execute(
            extract.JDBCExtractStage(
              plugin=new extract.JDBCExtract,
              id=None,
              name="dataset",
              description=None,
              schema=Right(Nil),
              outputView="expected",
              jdbcURL=mysqlURL,
              driver=DriverManager.getDriver(mysqlURL),
              tableName=s"inventory.${tableName}",
              numPartitions=None,
              fetchsize=None,
              partitionBy=Nil,
              customSchema=None,
              persist=true,
              partitionColumn=None,
              predicates=Nil,
              params=Map.empty
            )
          ).get
          assert(TestUtils.datasetEquality(expected, spark.table(tableName)))

          println(s"executed ${transactions.length} transactions against ${tableName} with ${update} updates, ${insert} inserts, ${delete} deletes\n")
        } catch {
          case e: Exception => fail(e.getMessage)
        } finally {
          TestHelpers.deleteConnector(connectURI, connectorName)
          TestHelpers.deleteConnector(connectURI, s"${connectorName}-dbhistory")
          ai.tripl.arc.execute.JDBCExecuteStage.execute(
            ai.tripl.arc.execute.JDBCExecuteStage(
              plugin=new ai.tripl.arc.execute.JDBCExecute,
              id=None,
              name="JDBCExecute",
              description=None,
              inputURI=new URI(mysqlURL),
              jdbcURL=mysqlURL,
              sql=makeTransaction(Seq(s"DROP TABLE inventory.${tableName};")),
              params=Map.empty,
              sqlParams=Map.empty
            )
          )
          writeStream.stop
        }
      }
    }
  }

  test("MySQLDebeziumTransform: Types") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming = true)

    val knownData = TestUtils.getKnownDataset.drop("nullDatum")
    val knownDataMetadata = MetadataUtils.createMetadataDataframe(knownData)
    knownDataMetadata.persist
    knownDataMetadata.createOrReplaceTempView(schema)

    val tableName = s"customers_${UUID.randomUUID.toString.replaceAll("-","")}"

    ai.tripl.arc.execute.JDBCExecuteStage.execute(
      ai.tripl.arc.execute.JDBCExecuteStage(
        plugin=new ai.tripl.arc.execute.JDBCExecute,
        id=None,
        name="JDBCExecute",
        description=None,
        inputURI=new URI(mysqlURL),
        jdbcURL=mysqlURL,
        sql=makeTransaction(Seq(s"CREATE TABLE ${tableName} (booleanDatum BOOLEAN NOT NULL, dateDatum DATE NOT NULL, decimalDatum DECIMAL(10,3) NOT NULL, doubleDatum DOUBLE NOT NULL, integerDatum INTEGER NOT NULL, longDatum BIGINT NOT NULL, stringDatum VARCHAR(255) NOT NULL, timeDatum VARCHAR(255) NOT NULL, timestampDatum TIMESTAMP NOT NULL);")),
        params=Map.empty,
        sqlParams=Map.empty
      )
    )

    knownData.write.mode("append").jdbc(mysqlURL, s"inventory.${tableName}", new java.util.Properties)

    TestHelpers.registerConnector(connectURI, makeConnectorConfig(s"inventory.${tableName}", "integerDatum"))

    val readStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", s"dbserver2.inventory.${tableName}")
      .option("startingOffsets", "earliest")
      .load
    readStream.createOrReplaceTempView(inputView)

    transform.DebeziumTransformStage.execute(
      transform.DebeziumTransformStage(
        plugin=new transform.DebeziumTransform,
        id=None,
        name="DebeziumTransform",
        description=None,
        inputView=inputView,
        outputView=outputView,
        schema=Left(schema),
        strict=true
      )
    )

    val writeStream = spark.table(outputView)
      .writeStream
      .outputMode("complete")
      .queryName(tableName)
      .format("memory")
      .start

    try {
      // wait for query to start
      val start = System.currentTimeMillis()
      while (writeStream.lastProgress == null || (writeStream.lastProgress != null && writeStream.lastProgress.numInputRows == 0)) {
        if (System.currentTimeMillis() > start + 180000) throw new Exception("Timeout without messages arriving")
        println("Waiting for query progress...")
        Thread.sleep(1000)
      }

      writeStream.processAllAvailable
      writeStream.stop

      // validate results
      assert(TestUtils.datasetEquality(knownData, spark.table(tableName)))
    } catch {
      case e: Exception => fail(e.getMessage)
    } finally {
      TestHelpers.deleteConnector(connectURI, connectorName)
      TestHelpers.deleteConnector(connectURI, s"${connectorName}-dbhistory")
      ai.tripl.arc.execute.JDBCExecuteStage.execute(
        ai.tripl.arc.execute.JDBCExecuteStage(
          plugin=new ai.tripl.arc.execute.JDBCExecute,
          id=None,
          name="JDBCExecute",
          description=None,
          inputURI=new URI(mysqlURL),
          jdbcURL=mysqlURL,
          sql=makeTransaction(Seq(s"DROP TABLE inventory.${tableName};")),
          params=Map.empty,
          sqlParams=Map.empty
        )
      )
      writeStream.stop
    }
  }

}