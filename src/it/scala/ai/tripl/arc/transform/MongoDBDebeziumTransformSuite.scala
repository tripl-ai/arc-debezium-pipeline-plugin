package ai.tripl.arc

import java.net.URI
import java.sql.DriverManager
import java.util.UUID

import scala.collection.JavaConverters._
import scala.util.Random

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils

import org.bson._
import org.bson.types.Decimal128
import com.mongodb._
import com.mongodb.client._
import com.mongodb.spark.config._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.StreamingQueryListener._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util.log.LoggerFactory
import ai.tripl.arc.udf.UDF

import ai.tripl.arc.util._

class MongoDBDebeziumTransformSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _

  val inputView = "inputView"
  val outputView = "outputView"
  val schema = "schema"
  val checkpointLocation = "/tmp/debezium"
  val serverName = "dbserver3"
  val size = 5000

  val database = "inventory"
  val mongoClientURI = s"mongodb://debezium:dbz@mongodb:27017/${database}?authSource=admin&replicaSet=rs0"
  val connectURI = s"http://connect:8083/connectors/"
  val connectorName = "inventory-connector-mongo"
  val kafkaBootstrap = "kafka:9092"

  val connectorConfig = s"""{
  |  "name": "${connectorName}",
  |  "config": {
  |    "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
  |    "tasks.max": "1",
  |    "mongodb.hosts" : "rs0/mongodb:27017",
  |    "mongodb.name" : "${serverName}",
  |    "mongodb.user" : "debezium",
  |    "mongodb.password" : "dbz",
  |    "database.whitelist": "inventory",
  |    "database.history.kafka.bootstrap.servers": "${kafkaBootstrap}",
  |    "database.history.kafka.topic": "schema-changes.inventory",
  |    "decimal.handling.mode": "string",
  |    "include.query": false,
  |    "enable.time.adjuster": true
  |  }
  |}""".stripMargin

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

    val drivers = DriverManager.getDrivers.asScala.toList.map(driver => driver.getClass.getName)

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

  sealed trait Operation
  object Operation {
    case object INSERT extends Operation
    case object UPDATE extends Operation
    case object DELETE extends Operation
  }

  case class Transaction(
    operation: Operation,
    filter: Option[BsonDocument],
    document: Option[BsonDocument],
  )

  def makeCustomerTransactions(customersInitial: Dataset[ai.tripl.arc.util.Customer], customersUpdates: Seq[ai.tripl.arc.util.Customer], seed: Int, limit: Int = Int.MaxValue): (Seq[Seq[Transaction]], Int, Int, Int) = {

    val random = new Random(seed)

    val customersUpdatesShuffle = random.shuffle(customersUpdates).take(limit)

    val existingIds = customersInitial.collect.map { customer => customer.c_custkey }.toSeq

    var transactions = Seq[Seq[Transaction]]()
    var i = 0
    var updates = 0
    var inserts = 0
    var deletes = 0

    while (i < customersUpdatesShuffle.length) {
      val len = (random.nextGaussian.abs * 5).ceil.toInt
      val transaction = customersUpdatesShuffle.drop(i).take(len).flatMap { customer =>
        random.nextInt(4) match {
          // update
          case 0 => {
            updates += 1
            Seq(
              Transaction(
                Operation.UPDATE,
                Option(new BsonDocument().append("_id", new BsonInt64(customer.c_custkey))),
                Option(new BsonDocument().append("$set", new BsonDocument().append("c_name", new BsonString(customer.c_name)).append("c_address", new BsonString(customer.c_address)).append("c_nationkey", new BsonInt32(customer.c_nationkey)).append("c_phone", new BsonString(customer.c_phone)).append("c_acctbal", new BsonDecimal128(new Decimal128(customer.c_acctbal.toJavaBigDecimal))).append("c_mktsegment", new BsonString(customer.c_mktsegment)).append("c_comment", new BsonString(customer.c_comment)))),
              ),
            )
          }
          // delete
          case 1 => {
            if (existingIds.contains(customer.c_custkey)) {
              deletes += 1
              Seq(
                Transaction(
                  Operation.DELETE,
                  Option(new BsonDocument().append("_id", new BsonInt64(customer.c_custkey))),
                  None,
                ),
              )
            } else {
              inserts += 1
              Seq(
                Transaction(
                  Operation.INSERT,
                  None,
                  Option(new BsonDocument().append("_id", new BsonInt64(customer.c_custkey)).append("c_name", new BsonString(customer.c_name)).append("c_address", new BsonString(customer.c_address)).append("c_nationkey", new BsonInt32(customer.c_nationkey)).append("c_phone", new BsonString(customer.c_phone)).append("c_acctbal", new BsonDecimal128(new Decimal128(customer.c_acctbal.toJavaBigDecimal))).append("c_mktsegment", new BsonString(customer.c_mktsegment)).append("c_comment", new BsonString(customer.c_comment))),
                ),
              )
            }
          }
          // delete then insert
          case 2 => {
            if (existingIds.contains(customer.c_custkey)) {
              deletes += 1
              inserts += 1
              Seq(
                Transaction(
                  Operation.DELETE,
                  Option(new BsonDocument().append("_id", new BsonInt64(customer.c_custkey))),
                  None,
                ),
                Transaction(
                  Operation.INSERT,
                  None,
                  Option(new BsonDocument().append("_id", new BsonInt64(customer.c_custkey)).append("c_name", new BsonString(customer.c_name)).append("c_address", new BsonString(customer.c_address)).append("c_nationkey", new BsonInt32(customer.c_nationkey)).append("c_phone", new BsonString(customer.c_phone)).append("c_acctbal", new BsonDecimal128(new Decimal128(customer.c_acctbal.toJavaBigDecimal))).append("c_mktsegment", new BsonString(customer.c_mktsegment)).append("c_comment", new BsonString(customer.c_comment))),
                ),
              )
            } else {
              inserts += 1
              Seq(
                Transaction(
                  Operation.INSERT,
                  None,
                  Option(new BsonDocument().append("_id", new BsonInt64(customer.c_custkey)).append("c_name", new BsonString(customer.c_name)).append("c_address", new BsonString(customer.c_address)).append("c_nationkey", new BsonInt32(customer.c_nationkey)).append("c_phone", new BsonString(customer.c_phone)).append("c_acctbal", new BsonDecimal128(new Decimal128(customer.c_acctbal.toJavaBigDecimal))).append("c_mktsegment", new BsonString(customer.c_mktsegment)).append("c_comment", new BsonString(customer.c_comment))),
                ),
              )
            }
          }
          // delete then insert + update (just swap first/last name)
          case 3 => {
            if (existingIds.contains(customer.c_custkey)) {
              deletes += 1
              inserts += 1
              updates += 1
              Seq(
                Transaction(
                  Operation.DELETE,
                  Option(new BsonDocument().append("_id", new BsonInt64(customer.c_custkey))),
                  None,
                ),
                Transaction(
                  Operation.INSERT,
                  None,
                  Option(new BsonDocument().append("_id", new BsonInt64(customer.c_custkey)).append("c_name", new BsonString(customer.c_name)).append("c_address", new BsonString(customer.c_address)).append("c_nationkey", new BsonInt32(customer.c_nationkey)).append("c_phone", new BsonString(customer.c_phone)).append("c_acctbal", new BsonDecimal128(new Decimal128(customer.c_acctbal.toJavaBigDecimal))).append("c_mktsegment", new BsonString(customer.c_mktsegment)).append("c_comment", new BsonString(customer.c_comment))),
                ),
                Transaction(
                  Operation.UPDATE,
                  Option(new BsonDocument().append("_id", new BsonInt64(customer.c_custkey))),
                  Option(new BsonDocument().append("$set", new BsonDocument().append("c_name", new BsonString(customer.c_name)).append("c_address", new BsonString(customer.c_address)).append("c_nationkey", new BsonInt32(customer.c_nationkey)).append("c_phone", new BsonString(customer.c_phone)).append("c_acctbal", new BsonDecimal128(new Decimal128(customer.c_acctbal.toJavaBigDecimal))).append("c_mktsegment", new BsonString(customer.c_mktsegment)).append("c_comment", new BsonString(customer.c_comment)))),
                ),
              )
            } else {
              inserts += 1
              updates += 1
              Seq(
                Transaction(
                  Operation.INSERT,
                  None,
                  Option(new BsonDocument().append("_id", new BsonInt64(customer.c_custkey)).append("c_name", new BsonString(customer.c_name)).append("c_address", new BsonString(customer.c_address)).append("c_nationkey", new BsonInt32(customer.c_nationkey)).append("c_phone", new BsonString(customer.c_phone)).append("c_acctbal", new BsonDecimal128(new Decimal128(customer.c_acctbal.toJavaBigDecimal))).append("c_mktsegment", new BsonString(customer.c_mktsegment)).append("c_comment", new BsonString(customer.c_comment))),
                ),
                Transaction(
                  Operation.UPDATE,
                  Option(new BsonDocument().append("_id", new BsonInt64(customer.c_custkey))),
                  Option(new BsonDocument().append("$set", new BsonDocument().append("c_name", new BsonString(customer.c_name)).append("c_address", new BsonString(customer.c_address)).append("c_nationkey", new BsonInt32(customer.c_nationkey)).append("c_phone", new BsonString(customer.c_phone)).append("c_acctbal", new BsonDecimal128(new Decimal128(customer.c_acctbal.toJavaBigDecimal))).append("c_mktsegment", new BsonString(customer.c_mktsegment)).append("c_comment", new BsonString(customer.c_comment)))),
                ),
              )
            }
          }
        }
      }

      transactions = transactions :+ transaction
      i += len
    }

    (transactions, updates, inserts, deletes)
  }

  test("MongoDBDebeziumTransform: Streaming") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming = true)

    val (customersInitial, customersUpdates) = TestHelpers.getTestCustomerData("customer.tbl.gz", size)
    val customersMetadata = MetadataUtils.createMetadataDataframe(customersInitial.toDF.withColumnRenamed("c_custkey", "_id"))
    customersMetadata.persist
    customersMetadata.createOrReplaceTempView(schema)

    println()
    for (seed <- 0 to 0) {
      for (strict <- Seq(true)) {
        FileUtils.deleteQuietly(new java.io.File(checkpointLocation))
        val tableName = s"customers_${UUID.randomUUID.toString.replaceAll("-","")}"
        println(s"streaming mongo ${if (strict) "strict" else "not-strict"} seed: ${seed} target: ${tableName}")

        customersInitial.withColumnRenamed("c_custkey", "_id").write.format("com.mongodb.spark.sql").options(WriteConfig(Map("uri" -> mongoClientURI, "collection" -> tableName)).asOptions).save

        // make transactions
        val (transactions, update, insert, delete) = makeCustomerTransactions(customersInitial, customersUpdates, seed)

        TestHelpers.registerConnector(connectURI, connectorConfig)

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
            schema=Left(schema),
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

        val mongoClient = MongoClients.create(mongoClientURI)
        val mongoCollection = mongoClient.getDatabase(database).getCollection(tableName)

        try {
          // wait for query to start
          val start = System.currentTimeMillis()
          while (writeStream.lastProgress == null || (writeStream.lastProgress != null && writeStream.lastProgress.numInputRows == 0)) {
            if (System.currentTimeMillis() > start + 60000) throw new Exception("Timeout without messages arriving")
            println("Waiting for query progress...")
            Thread.sleep(1000)
          }

          // while running perform PARALLEL insert/update/delete transactions
          // this will block the main thread but we want to process all updates before triggering awaitTermination
          var last = System.currentTimeMillis()
          var i = 0
          transactions.par.foreach { transaction =>
            if (System.currentTimeMillis() > last+1000) {
              last = System.currentTimeMillis()
              println(s"${i} transactions/sec")
              i = 0
            }
            i += 1
            val mongoSession = mongoClient.startSession(ClientSessionOptions.builder.defaultTransactionOptions(TransactionOptions.builder.readConcern(ReadConcern.MAJORITY).writeConcern(WriteConcern.MAJORITY).build).build)
            mongoSession.startTransaction
            transaction.foreach { t =>
              t.operation match {
                case Operation.INSERT => mongoCollection.insertOne(mongoSession, Document.parse(t.document.get.toJson))
                case Operation.UPDATE => mongoCollection.updateOne(mongoSession, t.filter.get, t.document.get)
                case Operation.DELETE => mongoCollection.deleteOne(mongoSession, t.filter.get)
                case _ =>
              }
            }
            mongoSession.commitTransaction
            mongoSession.close
          }
          println(s"executed ${transactions.length} transactions against ${tableName} with ${update} updates, ${insert} inserts, ${delete} deletes")

          Thread.sleep(5000)
          writeStream.processAllAvailable
          writeStream.stop

          // validate results
          val expected = spark.read.format("com.mongodb.spark.sql").options(WriteConfig(Map("uri" -> mongoClientURI, "collection" -> tableName)).asOptions).load
          expected.cache
          assert(expected.count > customersInitial.count)
          assert(TestUtils.datasetEquality(expected, spark.table(tableName).drop("_topic").drop("_offset")))
          println("PASS\n")

        } catch {
          case e: Exception => fail(e.getMessage)
        } finally {
          TestHelpers.deleteConnector(connectURI, connectorName)
          mongoCollection.drop
          writeStream.stop
        }
      }
    }
    customersInitial.unpersist
  }

  test("MongoDBDebeziumTransform: Types") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming = true)

    val knownData = TestUtils.getKnownDataset.withColumnRenamed("integerDatum", "_id").drop("nullDatum")
    val knownDataMetadata = MetadataUtils.createMetadataDataframe(knownData)
    knownDataMetadata.persist
    knownDataMetadata.createOrReplaceTempView(schema)

    println()
    val tableName = s"customers_${UUID.randomUUID.toString.replaceAll("-","")}"

    knownData.withColumnRenamed("integerDatum", "_id").write.format("com.mongodb.spark.sql").options(WriteConfig(Map("uri" -> mongoClientURI, "collection" -> tableName)).asOptions).save

    TestHelpers.registerConnector(connectURI, connectorConfig)

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
        schema=Left(schema),
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
        if (System.currentTimeMillis() > start + 60000) throw new Exception("Timeout without messages arriving")
        println("Waiting for query progress...")
        Thread.sleep(1000)
      }

      writeStream.processAllAvailable
      writeStream.stop

      // validate results
      assert(TestUtils.datasetEquality(knownData, spark.table(tableName).drop("_topic").drop("_offset")))
    } catch {
      case e: Exception => fail(e.getMessage)
    } finally {
      TestHelpers.deleteConnector(connectURI, connectorName)
      val mongoClient = MongoClients.create(mongoClientURI)
      val mongoCollection = mongoClient.getDatabase(database).getCollection(tableName)
      mongoCollection.drop
      writeStream.stop
    }
  }

}