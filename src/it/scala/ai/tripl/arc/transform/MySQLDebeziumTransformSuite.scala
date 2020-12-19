package ai.tripl.arc

import java.net.URI
import java.sql.DriverManager
import java.text.DecimalFormat
import java.util.UUID

import scala.collection.JavaConverters._
import scala.util.Random
import scala.util.control.Breaks._

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.StreamingQueryListener._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util.log.LoggerFactory
import ai.tripl.arc.udf.UDF
import ai.tripl.arc.transform.DebeziumStringKafkaEvent
import ai.tripl.arc.util.ControlUtils

import ai.tripl.arc.util._

class MySQLDebeziumTransformSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _

  val inputView = "inputView"
  val outputView = "outputView"
  val initialStateView = "initialStateView"
  val customersSchema = "customersSchema"
  val ordersSchema = "ordersSchema"
  val checkpointLocation = "/tmp/debezium"
  val serverName = "dbserver1"
  val size = 5000

  val databaseURL = "jdbc:mysql://mysql:3306/inventory?user=root&password=debezium&allowMultiQueries=true&rewriteBatchedStatements=true"
  val connectorName = "inventory-connector-mysql"
  val connectURI = s"http://connect:8083/connectors/"
  val kafkaBootstrap = "kafka:9092"

  before {
    implicit val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.ui.port", "4040")
      .config("spark.checkpoint.compress", "true")
      .config("spark.sql.shuffle.partitions", 4)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "2047m")
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
        println(s"numRowsTotal: ${if (queryProgress.progress.stateOperators.length == 0) 0 else queryProgress.progress.stateOperators(0).numRowsTotal} inputRowsPerSecond: ${queryProgress.progress.inputRowsPerSecond.round} processedRowsPerSecond: ${queryProgress.progress.processedRowsPerSecond.round}")
      }
    })

    FileUtils.deleteQuietly(new java.io.File(checkpointLocation))
  }

  after {
    FileUtils.deleteQuietly(new java.io.File(checkpointLocation))
    session.stop
  }

  def makeConnectorConfig(tables: String): String = {
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
    |    "database.server.name": "${serverName}",
    |    "database.whitelist": "inventory",
    |    "database.history.kafka.bootstrap.servers": "${kafkaBootstrap}",
    |    "database.history.kafka.topic": "schema-changes.inventory",
    |    "message.key.columns": "${tables}",
    |    "decimal.handling.mode": "string",
    |    "bigint.unsigned.handling.mode": "long",
    |    "include.query": false,
    |    "enable.time.adjuster": true
    |  }
    |}""".stripMargin
  }

  def makeTransaction(statements: Seq[String]): String = {
    s"""SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
    |START TRANSACTION;
    |${statements.mkString("\n")}
    |COMMIT;
    """.stripMargin
  }

  def makeCustomersTransactions(customersInitial: Dataset[ai.tripl.arc.util.Customer], customersUpdates: Seq[ai.tripl.arc.util.Customer], tableName: String, seed: Int, limit: Int = Int.MaxValue): (Seq[String], Int, Int, Int) = {

    val random = new Random(seed)

    val customersUpdatesShuffle = random.shuffle(customersUpdates).take(limit)

    val existingIds = customersInitial.collect.map { customer => customer.c_custkey }.toSeq

    var transactions = Seq[String]()
    var i = 0
    var updates = 0
    var inserts = 0
    var deletes = 0

    while (i < customersUpdatesShuffle.length) {
      val len = (random.nextGaussian.abs * 5).ceil.toInt
      val transaction = makeTransaction(
      customersUpdatesShuffle.drop(i).take(len).flatMap { customer =>
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

  def makeOrdersTransactions(ordersInitial: Dataset[ai.tripl.arc.util.Order], ordersUpdates: Seq[ai.tripl.arc.util.Order], tableName: String, seed: Int, limit: Int = Int.MaxValue): (Seq[String], Int, Int, Int) = {

    val random = new Random(seed)

    val ordersUpdatesShuffle = random.shuffle(ordersUpdates).take(limit)

    val existingIds = ordersInitial.collect.map { order => order.o_orderkey }.toSeq

    var transactions = Seq[String]()
    var i = 0
    var updates = 0
    var inserts = 0
    var deletes = 0

    while (i < ordersUpdatesShuffle.length) {
      val len = (random.nextGaussian.abs * 5).ceil.toInt
      val transaction = makeTransaction(
      ordersUpdatesShuffle.drop(i).take(len).flatMap { order =>
        random.nextInt(4) match {
          // full update
          case 0 => {
            updates += 1
            Seq(s"UPDATE ${tableName} SET o_custkey=${order.o_custkey}, o_orderstatus='${order.o_orderstatus}', o_totalprice=${order.o_totalprice}, o_orderdate='${order.o_orderdate.toString}', o_orderpriority='${order.o_orderpriority}', o_clerk='${order.o_clerk}', o_shippriority=${order.o_shippriority}, o_comment='${order.o_comment}' WHERE o_orderkey=${order.o_orderkey};")
          }
          // delete
          case 1 => {
            if (existingIds.contains(order.o_orderkey)) {
              deletes += 1
              Seq(s"DELETE FROM ${tableName} WHERE o_orderkey=${order.o_orderkey};")
            } else {
              inserts += 1
              Seq(s"INSERT INTO ${tableName} (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment) VALUES (${order.o_orderkey}, ${order.o_custkey}, '${order.o_orderstatus}', ${order.o_totalprice}, '${order.o_orderdate.toString}', '${order.o_orderpriority}', '${order.o_clerk}', ${order.o_shippriority}, '${order.o_comment}');")
            }
          }
          // delete then insert
          case 2 => {
            if (existingIds.contains(order.o_orderkey)) {
              deletes += 1
              inserts += 1
              Seq(
                s"DELETE FROM ${tableName} WHERE o_orderkey=${order.o_orderkey};",
                s"INSERT INTO ${tableName} (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment) VALUES (${order.o_orderkey}, ${order.o_custkey}, '${order.o_orderstatus}', ${order.o_totalprice}, '${order.o_orderdate.toString}', '${order.o_orderpriority}', '${order.o_clerk}', ${order.o_shippriority}, '${order.o_comment}');",
              )
            } else {
              inserts += 1
              Seq(s"INSERT INTO ${tableName} (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment) VALUES (${order.o_orderkey}, ${order.o_custkey}, '${order.o_orderstatus}', ${order.o_totalprice}, '${order.o_orderdate.toString}', '${order.o_orderpriority}', '${order.o_clerk}', ${order.o_shippriority}, '${order.o_comment}');")
            }
          }
          // delete then insert + update (just swap first/last name)
          case 3 => {
            if (existingIds.contains(order.o_orderkey)) {
              deletes += 1
              inserts += 1
              updates += 1
              Seq(
                s"DELETE FROM ${tableName} WHERE o_orderkey=${order.o_orderkey};",
                s"INSERT INTO ${tableName} (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment) VALUES (${order.o_orderkey}, ${order.o_custkey}, '${order.o_orderstatus}', ${order.o_totalprice}, '${order.o_orderdate.toString}', '${order.o_orderpriority}', '${order.o_clerk}', ${order.o_shippriority}, '${order.o_comment}');",
                s"UPDATE ${tableName} SET o_custkey=${order.o_custkey}, o_orderstatus='${order.o_orderstatus}', o_totalprice=${order.o_totalprice}, o_orderdate='${order.o_orderdate.toString}', o_orderpriority='${order.o_orderpriority}', o_clerk='${order.o_clerk}', o_shippriority=${order.o_shippriority}, o_comment='${order.o_comment}' WHERE o_orderkey=${order.o_orderkey};",
              )
            } else {
              inserts += 1
              updates += 1
              Seq(
                s"INSERT INTO ${tableName} (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment) VALUES (${order.o_orderkey}, ${order.o_custkey}, '${order.o_orderstatus}', ${order.o_totalprice}, '${order.o_orderdate.toString}', '${order.o_orderpriority}', '${order.o_clerk}', ${order.o_shippriority}, '${order.o_comment}');",
                s"UPDATE ${tableName} SET o_custkey=${order.o_custkey}, o_orderstatus='${order.o_orderstatus}', o_totalprice=${order.o_totalprice}, o_orderdate='${order.o_orderdate.toString}', o_orderpriority='${order.o_orderpriority}', o_clerk='${order.o_clerk}', o_shippriority=${order.o_shippriority}, o_comment='${order.o_comment}' WHERE o_orderkey=${order.o_orderkey};",
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

  test("MySQLDebeziumTransform: Streaming") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming = true)

    val (customersInitial, customersUpdates) = TestHelpers.getTestCustomerData("customer.tbl.gz", size)
    val customersMetadata = MetadataUtils.createMetadataDataframe(customersInitial.toDF)
    customersMetadata.persist
    customersMetadata.createOrReplaceTempView(customersSchema)

    println()
    for (seed <- 0 to 0) {
      for (strict <- Seq(true, false)) {
        FileUtils.deleteQuietly(new java.io.File(checkpointLocation))
        val tableName = s"customers_${UUID.randomUUID.toString.replaceAll("-","")}"
        println(s"streaming mysql ${if (strict) "strict" else "not-strict"} seed: ${seed} target: ${tableName}")

        ai.tripl.arc.execute.JDBCExecuteStage.execute(
          ai.tripl.arc.execute.JDBCExecuteStage(
            plugin=new ai.tripl.arc.execute.JDBCExecute,
            id=None,
            name="JDBCExecute",
            description=None,
            inputURI=new URI(databaseURL),
            jdbcURL=databaseURL,
            sql=makeTransaction(Seq(s"CREATE TABLE ${tableName} (c_custkey BIGINT NOT NULL, c_name VARCHAR(25) NOT NULL, c_address VARCHAR(40) NOT NULL, c_nationkey INTEGER NOT NULL, c_phone VARCHAR(15) NOT NULL, c_acctbal DECIMAL(20,2) NOT NULL, c_mktsegment VARCHAR(10) NOT NULL, c_comment VARCHAR(117) NOT NULL);")),
            params=Map.empty,
            sqlParams=Map.empty
          )
        )
        customersInitial.write.mode("append").jdbc(databaseURL, tableName, new java.util.Properties)
        ai.tripl.arc.execute.JDBCExecuteStage.execute(
          ai.tripl.arc.execute.JDBCExecuteStage(
            plugin=new ai.tripl.arc.execute.JDBCExecute,
            id=None,
            name="JDBCExecute",
            description=None,
            inputURI=new URI(databaseURL),
            jdbcURL=databaseURL,
            sql=makeTransaction(Seq(s"ALTER TABLE ${tableName} ADD PRIMARY KEY (c_custkey);")),
            params=Map.empty,
            sqlParams=Map.empty
          )
        )

        // make transactions
        val (transactions, update, insert, delete) = makeCustomersTransactions(customersInitial, customersUpdates, tableName, seed)

        TestHelpers.registerConnector(connectURI, makeConnectorConfig(s"inventory.${tableName}:c_custkey"))

        val readStream = spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "kafka:9092")
          .option("subscribe", s"${serverName}.inventory.${tableName}")
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
            schema=Left(customersSchema),
            strict=strict,
            initialStateView=None,
            initialStateKey=None,
            persist=false,
            numPartitions=None,
            partitionBy=List.empty,
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
            if (System.currentTimeMillis() > start + 30000) throw new Exception("Timeout without messages arriving")
            println("Waiting for query progress...")
            Thread.sleep(1000)
          }

          // while running perform SERIAL insert/update/delete transactions
          // this will block the main thread but we want to process all updates before triggering awaitTermination
          var last = System.currentTimeMillis()
          var i = 0
          var deadlocks = 0
          ControlUtils.using(DriverManager.getConnection(databaseURL, new java.util.Properties)) { connection =>
            ControlUtils.using(connection.createStatement) { stmt =>
              transactions.foreach { sql =>
                if (System.currentTimeMillis() > last+1000) {
                  last = System.currentTimeMillis()
                  println(s"${i} transactions/sec (${deadlocks} deadlocks)")
                  i = 0
                }
                i += 1
                var retry = 0
                breakable {
                  while(true){
                    if (retry == 100) {
                      throw new Exception("could not complete transaction due to deadlocks")
                      break
                    }
                    try {
                        val res = stmt.execute(sql)
                        // try to get results to throw error if one exists
                        if (res) {
                          stmt.getResultSet.next
                        }
                      break
                    } catch {
                      case e: Exception if e.getMessage.contains("Deadlock found") => {
                        retry += 1
                        deadlocks += 1
                        Thread.sleep(200)
                      }
                    }
                  }
                }
              }
            }
          }
          println(s"executed ${transactions.length} transactions (${deadlocks} deadlocks) against ${tableName} with ${update} updates, ${insert} inserts, ${delete} deletes.")

          Thread.sleep(5000)
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
              jdbcURL=databaseURL,
              driver=DriverManager.getDriver(databaseURL),
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
          assert(TestUtils.datasetEquality(expected, spark.table(tableName).drop("_topic").drop("_offset")))
          println("PASS\n")

        } catch {
          case e: Exception => fail(e.getMessage)
        } finally {
          TestHelpers.deleteConnector(connectURI, "inventory-connector-mysql")
          TestHelpers.deleteConnector(connectURI, "inventory-connector-mysql-dbhistory")
          ai.tripl.arc.execute.JDBCExecuteStage.execute(
            ai.tripl.arc.execute.JDBCExecuteStage(
              plugin=new ai.tripl.arc.execute.JDBCExecute,
              id=None,
              name="JDBCExecute",
              description=None,
              inputURI=new URI(databaseURL),
              jdbcURL=databaseURL,
              sql=makeTransaction(Seq(s"DROP TABLE inventory.${tableName};")),
              params=Map.empty,
              sqlParams=Map.empty
            )
          )
          writeStream.stop
        }
      }
    }
    customersInitial.unpersist
  }

  test("MySQLDebeziumTransform: Types") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming = true)

    val knownData = TestUtils.getKnownDatasetMySQL.drop("nullDatum")
    val schema = ai.tripl.arc.util.ArcSchema.parseArcSchema(TestUtils.getKnownDatasetMetadataJsonMySQL)

    val tableName = s"customers_${UUID.randomUUID.toString.replaceAll("-","")}"

    ai.tripl.arc.execute.JDBCExecuteStage.execute(
      ai.tripl.arc.execute.JDBCExecuteStage(
        plugin=new ai.tripl.arc.execute.JDBCExecute,
        id=None,
        name="JDBCExecute",
        description=None,
        inputURI=new URI(databaseURL),
        jdbcURL=databaseURL,
        sql=makeTransaction(Seq(s"CREATE TABLE ${tableName} (booleanDatum BOOLEAN NOT NULL, dateDatum DATE NOT NULL, decimalDatum DECIMAL(10,3) NOT NULL, doubleDatum DOUBLE NOT NULL, integerDatum INTEGER NOT NULL, longDatum BIGINT NOT NULL, bigIntDatum BIGINT UNSIGNED NOT NULL, stringDatum VARCHAR(255) NOT NULL, timeDatum VARCHAR(255) NOT NULL, timestampDatum TIMESTAMP NOT NULL);")),
        params=Map.empty,
        sqlParams=Map.empty
      )
    )
    knownData.write.mode("append").jdbc(databaseURL, s"inventory.${tableName}", new java.util.Properties)

    TestHelpers.registerConnector(connectURI, makeConnectorConfig(s"inventory.${tableName}:integerDatum"))

    val readStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", s"${serverName}.inventory.${tableName}")
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
        schema=Right(schema.right.getOrElse(Nil)),
        strict=true,
        initialStateView=None,
        initialStateKey=None,
        persist=false,
        numPartitions=None,
        partitionBy=List.empty,
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
        if (System.currentTimeMillis() > start + 30000) throw new Exception("Timeout without messages arriving")
        println("Waiting for query progress...")
        Thread.sleep(1000)
      }

      Thread.sleep(5000)
      writeStream.processAllAvailable
      writeStream.stop

      // validate results
      assert(spark.table(tableName).select("_offset").collect.forall(row => Seq(0L,1L).contains(row.getLong(0))))
      assert(TestUtils.datasetEquality(knownData.withColumn("bigIntDatum", col("bigIntDatum").cast(DecimalType(38,0))).withColumn("_topic", lit(s"dbserver1.inventory.${tableName}")), spark.table(tableName).drop("_offset")))
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
          inputURI=new URI(databaseURL),
          jdbcURL=databaseURL,
          sql=makeTransaction(Seq(s"DROP TABLE inventory.${tableName};")),
          params=Map.empty,
          sqlParams=Map.empty
        )
      )
      writeStream.stop
    }
  }

  test("MySQLDebeziumTransform: Batch") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming = true)

    val (customersInitial, customersUpdates) = TestHelpers.getTestCustomerData("customer.tbl.gz", size)
    val customersMetadata = MetadataUtils.createMetadataDataframe(customersInitial.toDF)
    customersMetadata.persist
    customersMetadata.createOrReplaceTempView(customersSchema)

    println()
    for (seed <- 0 to 0) {
      for (strict <- Seq(true, false)) {
        val tableName = s"customers_${UUID.randomUUID.toString.replaceAll("-","")}"
        println(s"batch mysql ${if (strict) "strict" else "not-strict"} seed: ${seed} target: ${tableName}")

        ai.tripl.arc.execute.JDBCExecuteStage.execute(
          ai.tripl.arc.execute.JDBCExecuteStage(
            plugin=new ai.tripl.arc.execute.JDBCExecute,
            id=None,
            name="JDBCExecute",
            description=None,
            inputURI=new URI(databaseURL),
            jdbcURL=databaseURL,
            sql=makeTransaction(Seq(s"CREATE TABLE ${tableName} (c_custkey INTEGER NOT NULL, c_name VARCHAR(25) NOT NULL, c_address VARCHAR(40) NOT NULL, c_nationkey INTEGER NOT NULL, c_phone VARCHAR(15) NOT NULL, c_acctbal DECIMAL(20,2) NOT NULL, c_mktsegment VARCHAR(10) NOT NULL, c_comment VARCHAR(117) NOT NULL);")),
            params=Map.empty,
            sqlParams=Map.empty
          )
        )
        customersInitial.write.mode("append").jdbc(databaseURL, tableName, new java.util.Properties)
        ai.tripl.arc.execute.JDBCExecuteStage.execute(
          ai.tripl.arc.execute.JDBCExecuteStage(
            plugin=new ai.tripl.arc.execute.JDBCExecute,
            id=None,
            name="JDBCExecute",
            description=None,
            inputURI=new URI(databaseURL),
            jdbcURL=databaseURL,
            sql=makeTransaction(Seq(s"ALTER TABLE ${tableName} ADD PRIMARY KEY (c_custkey);")),
            params=Map.empty,
            sqlParams=Map.empty
          )
        )

        // make transactions
        val (transactions, update, insert, delete) = makeCustomersTransactions(customersInitial, customersUpdates, tableName, seed)

        TestHelpers.registerConnector(connectURI, makeConnectorConfig(s"inventory.${tableName}:c_custkey"))

        val readStream = spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "kafka:9092")
          .option("subscribe", s"${serverName}.inventory.${tableName}")
          .option("startingOffsets", "earliest")
          .load
        readStream.createOrReplaceTempView(inputView)

        val writeStream = readStream
          .writeStream
          .outputMode("append")
          .queryName(tableName)
          .format("memory")
          .start

        try {
          // wait for query to start
          val start = System.currentTimeMillis()
          while (writeStream.lastProgress == null || (writeStream.lastProgress != null && writeStream.lastProgress.numInputRows == 0)) {
            if (System.currentTimeMillis() > start + 30000) throw new Exception("Timeout without messages arriving")
            println("Waiting for query progress...")
            Thread.sleep(1000)
          }

          // while running perform SERIAL insert/update/delete transactions
          // this will block the main thread but we want to process all updates before triggering awaitTermination
          var last = System.currentTimeMillis()
          var i = 0
          var deadlocks = 0
          ControlUtils.using(DriverManager.getConnection(databaseURL, new java.util.Properties)) { connection =>
            ControlUtils.using(connection.createStatement) { stmt =>
              transactions.foreach { sql =>
                if (System.currentTimeMillis() > last+1000) {
                  last = System.currentTimeMillis()
                  println(s"${i} transactions/sec (${deadlocks} deadlocks)")
                  i = 0
                }
                i += 1
                var retry = 0
                breakable {
                  while(true){
                    if (retry == 100) {
                      throw new Exception("could not complete transaction due to deadlocks")
                      break
                    }
                    try {
                        val res = stmt.execute(sql)
                        // try to get results to throw error if one exists
                        if (res) {
                          stmt.getResultSet.next
                        }
                      break
                    } catch {
                      case e: Exception if e.getMessage.contains("Deadlock found") => {
                        retry += 1
                        deadlocks += 1
                        Thread.sleep(200)
                      }
                    }
                  }
                }
              }
            }
          }

          println(s"executed ${transactions.length} transactions (${deadlocks} deadlocks) against ${tableName} with ${update} updates, ${insert} inserts, ${delete} deletes.")

          Thread.sleep(5000)
          writeStream.processAllAvailable
          writeStream.stop

          // read in batch mode
          val read = spark
            .read
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", s"${serverName}.inventory.${tableName}")
            .option("startingOffsets", "earliest")
            .load
            .as[DebeziumStringKafkaEvent]
            .collect
            .sortBy(row => row.offset)
            .toList

          // make sure the updates have happened
          assert(read.size > customersInitial.count)

          // recursively apply the records passing in the previous state
          val batches = 3
          println(s"processing ${read.size} events in ${batches} ${read.size/batches} record batches...")
          read.grouped(read.size/batches).zipWithIndex.foreach { case (batch, index) =>
            val start = System.currentTimeMillis()
            batch.toDF.createOrReplaceTempView(inputView)
            transform.DebeziumTransformStage.execute(
              transform.DebeziumTransformStage(
                plugin=new transform.DebeziumTransform,
                id=None,
                name="DebeziumTransform",
                description=None,
                inputView=inputView,
                outputView=outputView,
                schema=Left(customersSchema),
                strict=strict,
                initialStateView=if (index == 0) None else Option(initialStateView),
                initialStateKey=Option("c_custkey"),
                persist=true,
                numPartitions=None,
                partitionBy=List.empty,
              )
            )
            val output = spark.table(outputView)
            output.createOrReplaceTempView(initialStateView)
            println(s"processed batch ${index} of ${batch.length} records in ${System.currentTimeMillis()-start}ms... new total ${output.count}")
          }

          // validate results
          val expected = extract.JDBCExtractStage.execute(
            extract.JDBCExtractStage(
              plugin=new extract.JDBCExtract,
              id=None,
              name="dataset",
              description=None,
              schema=Right(Nil),
              outputView="expected",
              jdbcURL=databaseURL,
              driver=DriverManager.getDriver(databaseURL),
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
          assert(TestUtils.datasetEquality(expected, spark.table(outputView).drop("_topic").drop("_offset")))
          println(s"PASS: expected: ${expected.count} actual: ${spark.table(outputView).count}\n")

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
              inputURI=new URI(databaseURL),
              jdbcURL=databaseURL,
              sql=makeTransaction(Seq(s"DROP TABLE inventory.${tableName};")),
              params=Map.empty,
              sqlParams=Map.empty
            )
          )
          writeStream.stop
        }
      }
    }
    customersInitial.unpersist
  }

  test("MySQLDebeziumTransform: Join") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming = true)

    val (customersInitial, customersUpdates) = TestHelpers.getTestCustomerData("customer_sm.tbl.gz")
    val customersMetadata = MetadataUtils.createMetadataDataframe(customersInitial.toDF)
    customersMetadata.persist
    customersMetadata.createOrReplaceTempView(customersSchema)

    val (ordersInitial, ordersUpdates) = TestHelpers.getTestOrdersData("orders_sm.tbl.gz")
    val ordersMetadata = MetadataUtils.createMetadataDataframe(ordersInitial.toDF)
    ordersMetadata.persist
    ordersMetadata.createOrReplaceTempView(ordersSchema)

    println()
    for (seed <- 0 to 0) {
      for (strict <- Seq(true)) {
        FileUtils.deleteQuietly(new java.io.File(checkpointLocation))
        val uuid = UUID.randomUUID.toString.replaceAll("-","")
        val customersTableName = s"customers_${uuid}"
        val ordersTableName = s"orders_${uuid}"
        println(s"streaming mysql ${if (strict) "strict" else "not-strict"} seed: ${seed} targets: [${customersTableName}, ${ordersTableName}]")

        ai.tripl.arc.execute.JDBCExecuteStage.execute(
          ai.tripl.arc.execute.JDBCExecuteStage(
            plugin=new ai.tripl.arc.execute.JDBCExecute,
            id=None,
            name="JDBCExecute",
            description=None,
            inputURI=new URI(databaseURL),
            jdbcURL=databaseURL,
            sql=makeTransaction(Seq(s"CREATE TABLE ${customersTableName} (c_custkey INTEGER NOT NULL, c_name VARCHAR(25) NOT NULL, c_address VARCHAR(40) NOT NULL, c_nationkey INTEGER NOT NULL, c_phone VARCHAR(15) NOT NULL, c_acctbal DECIMAL(20,2) NOT NULL, c_mktsegment VARCHAR(10) NOT NULL, c_comment VARCHAR(117) NOT NULL);")),
            params=Map.empty,
            sqlParams=Map.empty
          )
        )
        customersInitial.write.mode("append").jdbc(databaseURL, customersTableName, new java.util.Properties)
        ai.tripl.arc.execute.JDBCExecuteStage.execute(
          ai.tripl.arc.execute.JDBCExecuteStage(
            plugin=new ai.tripl.arc.execute.JDBCExecute,
            id=None,
            name="JDBCExecute",
            description=None,
            inputURI=new URI(databaseURL),
            jdbcURL=databaseURL,
            sql=makeTransaction(Seq(s"ALTER TABLE ${customersTableName} ADD PRIMARY KEY (c_custkey);")),
            params=Map.empty,
            sqlParams=Map.empty
          )
        )

        ai.tripl.arc.execute.JDBCExecuteStage.execute(
          ai.tripl.arc.execute.JDBCExecuteStage(
            plugin=new ai.tripl.arc.execute.JDBCExecute,
            id=None,
            name="JDBCExecute",
            description=None,
            inputURI=new URI(databaseURL),
            jdbcURL=databaseURL,
            sql=makeTransaction(Seq(s"CREATE TABLE ${ordersTableName} (o_orderkey INTEGER NOT NULL, o_custkey INTEGER NOT NULL, o_orderstatus VARCHAR(1) NOT NULL, o_totalprice DECIMAL(20,2) NOT NULL, o_orderdate DATE NOT NULL, o_orderpriority VARCHAR(15) NOT NULL, o_clerk VARCHAR(15) NOT NULL, o_shippriority INTEGER NOT NULL, o_comment VARCHAR(79) NOT NULL);")),
            params=Map.empty,
            sqlParams=Map.empty
          )
        )
        ordersInitial.write.mode("append").jdbc(databaseURL, ordersTableName, new java.util.Properties)
        ai.tripl.arc.execute.JDBCExecuteStage.execute(
          ai.tripl.arc.execute.JDBCExecuteStage(
            plugin=new ai.tripl.arc.execute.JDBCExecute,
            id=None,
            name="JDBCExecute",
            description=None,
            inputURI=new URI(databaseURL),
            jdbcURL=databaseURL,
            sql=makeTransaction(Seq(s"ALTER TABLE ${ordersTableName} ADD PRIMARY KEY (o_orderkey);")),
            params=Map.empty,
            sqlParams=Map.empty
          )
        )

        // make transactions
        val (customersTransactions, customersUpdate, customersInsert, customersDelete) = makeCustomersTransactions(customersInitial, customersUpdates, customersTableName, seed)
        val (ordersTransactions, ordersUpdate, ordersInsert, ordersDelete) = makeOrdersTransactions(ordersInitial, ordersUpdates, ordersTableName, seed)

        TestHelpers.registerConnector(connectURI, makeConnectorConfig(s"inventory.${customersTableName}:c_custkey;inventory.${ordersTableName}:o_orderkey"))

        val readStream0 = spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "kafka:9092")
          .option("subscribe", s"${serverName}.inventory.${customersTableName}")
          .option("startingOffsets", "earliest")
          .load

        val writeStream0 = readStream0
          .writeStream
          .outputMode("append")
          .queryName("writeStream0")
          .format("memory")
          .start

        val readStream1 = spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "kafka:9092")
          .option("subscribe", s"${serverName}.inventory.${ordersTableName}")
          .option("startingOffsets", "earliest")
          .load

        val writeStream1 = readStream1
          .writeStream
          .outputMode("append")
          .queryName("writeStream1")
          .format("memory")
          .start

        try {
          // wait for query to start
          val start = System.currentTimeMillis()
          while (
            (writeStream0.lastProgress == null || (writeStream0.lastProgress != null && writeStream0.lastProgress.numInputRows == 0)) &&
            (writeStream1.lastProgress == null || (writeStream1.lastProgress != null && writeStream1.lastProgress.numInputRows == 0))
          ) {
            if (System.currentTimeMillis() > start + 30000) throw new Exception("Timeout without messages arriving")
            println("Waiting for query progress...")
            Thread.sleep(1000)
          }

          // while running perform SERIAL insert/update/delete transactions
          // this will block the main thread but we want to process all updates before triggering awaitTermination
          var last = System.currentTimeMillis()
          var i = 0
          var deadlocks = 0
          ControlUtils.using(DriverManager.getConnection(databaseURL, new java.util.Properties)) { connection =>
            ControlUtils.using(connection.createStatement) { stmt =>
              for (query <- 0 to Math.max(customersTransactions.length, ordersTransactions.length)) {
                if (System.currentTimeMillis() > last+1000) {
                  last = System.currentTimeMillis()
                  println(s"${i} transactions/sec (${deadlocks} deadlocks)")
                  i = 0
                }
                i += 1

                // customers
                if (query < customersTransactions.length - 1) {
                  var retry = 0
                  breakable {
                    while(true){
                      if (retry == 100) {
                        throw new Exception("could not complete transaction due to deadlocks")
                        break
                      }
                      try {
                          val res = stmt.execute(customersTransactions(query))
                          // try to get results to throw error if one exists
                          if (res) {
                            stmt.getResultSet.next
                          }
                        break
                      } catch {
                        case e: Exception if e.getMessage.contains("Deadlock found") => {
                          retry += 1
                          deadlocks += 1
                          Thread.sleep(200)
                        }
                      }
                    }
                  }
                }

                // orders
                if (query < ordersTransactions.length - 1) {
                  var retry = 0
                  breakable {
                    while(true){
                      if (retry == 100) {
                        throw new Exception("could not complete transaction due to deadlocks")
                        break
                      }
                      try {
                          val res = stmt.execute(ordersTransactions(query))
                          // try to get results to throw error if one exists
                          if (res) {
                            stmt.getResultSet.next
                          }
                        break
                      } catch {
                        case e: Exception if e.getMessage.contains("Deadlock found") => {
                          retry += 1
                          deadlocks += 1
                          Thread.sleep(200)
                        }
                      }
                    }
                  }
                }
              }
            }
          }
          println(s"executed ${customersTransactions.length} transactions (${deadlocks} deadlocks) against ${customersTableName} with ${customersUpdate} updates, ${customersInsert} inserts, ${customersDelete} deletes.\nexecuted ${ordersTransactions.length} transactions (${deadlocks} deadlocks) against ${ordersTableName} with ${ordersUpdate} updates, ${ordersInsert} inserts, ${ordersDelete} deletes.")

          Thread.sleep(5000)
          writeStream0.processAllAvailable
          writeStream1.processAllAvailable
          writeStream0.stop
          writeStream1.stop

          // read in batch mode
          spark
            .read
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", s"${serverName}.inventory.${customersTableName}")
            .option("startingOffsets", "earliest")
            .load
            .as[DebeziumStringKafkaEvent]
            .collect
            .sortBy(row => row.offset)
            .toList
            .toDF
            .createOrReplaceTempView("customersEvents")

          transform.DebeziumTransformStage.execute(
            transform.DebeziumTransformStage(
              plugin=new transform.DebeziumTransform,
              id=None,
              name="DebeziumTransform",
              description=None,
              inputView="customersEvents",
              outputView="customersOutputView",
              schema=Left(customersSchema),
              strict=strict,
              initialStateView=None,
              initialStateKey=None,
              persist=true,
              numPartitions=None,
              partitionBy=List.empty,
            )
          )

          spark.table("customersOutputView").drop("_topic").drop("_offset").createOrReplaceTempView("customersOutputView")

          // read in batch mode
          spark
            .read
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", s"${serverName}.inventory.${ordersTableName}")
            .option("startingOffsets", "earliest")
            .load
            .as[DebeziumStringKafkaEvent]
            .collect
            .sortBy(row => row.offset)
            .toList
            .toDF
            .createOrReplaceTempView("ordersEvents")

          transform.DebeziumTransformStage.execute(
            transform.DebeziumTransformStage(
              plugin=new transform.DebeziumTransform,
              id=None,
              name="DebeziumTransform",
              description=None,
              inputView="ordersEvents",
              outputView="ordersOutputView",
              schema=Left(ordersSchema),
              strict=strict,
              initialStateView=None,
              initialStateKey=None,
              persist=true,
              numPartitions=None,
              partitionBy=List.empty,
            )
          )

          spark.table("ordersOutputView").drop("_topic").drop("_offset").createOrReplaceTempView("ordersOutputView")

          transform.SQLTransformStage.execute(
            transform.SQLTransformStage(
              plugin=new transform.SQLTransform,
              id=None,
              name="SQLTransform",
              description=None,
              inputURI=None,
              sql=s"SELECT * FROM customersOutputView INNER JOIN ordersOutputView ON customersOutputView.c_custkey=ordersOutputView.o_custkey",
              outputView="joined",
              persist=true,
              sqlParams=Map.empty,
              authentication=None,
              params=Map.empty,
              numPartitions=None,
              partitionBy=Nil
            )
          )

          // validate results
          val expected = extract.JDBCExtractStage.execute(
            extract.JDBCExtractStage(
              plugin=new extract.JDBCExtract,
              id=None,
              name="dataset",
              description=None,
              schema=Right(Nil),
              outputView="expected",
              jdbcURL=databaseURL,
              driver=DriverManager.getDriver(databaseURL),
              tableName=s"(SELECT * FROM ${customersTableName} INNER JOIN ${ordersTableName} ON ${customersTableName}.c_custkey=${ordersTableName}.o_custkey) joined",
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
          assert(TestUtils.datasetEquality(expected, spark.table("joined")))
          println("PASS\n")

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
              inputURI=new URI(databaseURL),
              jdbcURL=databaseURL,
              sql=makeTransaction(Seq(s"DROP TABLE inventory.${customersTableName};", s"DROP TABLE inventory.${ordersTableName};")),
              params=Map.empty,
              sqlParams=Map.empty
            )
          )
          writeStream0.stop
          writeStream1.stop
        }
      }
    }
    customersInitial.unpersist
  }

}