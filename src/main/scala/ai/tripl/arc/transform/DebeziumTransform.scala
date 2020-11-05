package ai.tripl.arc.transform

import java.io._
import java.net.URI
import java.util.Properties
import java.sql.DriverManager
import java.sql.Date
import java.time.Instant
import java.sql.Timestamp
import java.nio.charset.StandardCharsets

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Try

import com.typesafe.config._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.bson._
import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, GroupState}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.CloudUtils
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.ExtractUtils
import ai.tripl.arc.util.MetadataUtils
import ai.tripl.arc.util.Utils

class DebeziumTransform extends PipelineStagePlugin with JupyterCompleter {

  val version = ai.tripl.arc.debezium.BuildInfo.version

  val snippet = """{
    |}""".stripMargin

  val documentationURI = new java.net.URI(s"${baseURI}/transform/#debeziumtransform")

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "id" :: "name" :: "description" :: "environments" :: "inputView" :: "outputView" :: "authentication" :: "schema" :: "schemaURI" :: "schemaView" :: "strict" :: Nil
    val id = getOptionalValue[String]("id")
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val outputView = getValue[String]("outputView")
    val authentication = readAuthentication("authentication")
    val strict = getValue[java.lang.Boolean]("strict", default = Some(true))
    val source = checkOneOf(c)(Seq("schema", "schemaURI", "schemaView"))
    val (schema, schemaURI, schemaView) = if (source.isRight) {
      (
        if (c.hasPath("schema")) Right(c.getConfigList("schema").asScala.map { o => o.root().render(ConfigRenderOptions.concise()) }.mkString("[", ",", "]") ) |> verifyInlineSchemaPolicy("schema") _ |> getExtractColumns("schema") _ else Right(List.empty),
        if (c.hasPath("schemaURI")) getValue[String]("schemaURI") |> parseURI("schemaURI") _ |> textContentForURI("schemaURI", authentication) |> getExtractColumns("schemaURI") _ else Right(List.empty),
        if (c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")
      )
    } else {
      (Right(List.empty), Right(List.empty), Right(""))
    }

    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (id, name, description, inputView, outputView, schema, schemaURI, schemaView, authentication, strict, invalidKeys) match {
      case (Right(id), Right(name), Right(description), Right(inputView), Right(outputView), Right(schema), Right(schemaURI), Right(schemaView), Right(authentication), Right(strict), Right(invalidKeys)) =>
        val _schema = if (c.hasPath("schemaView")) {
          Left(schemaView)
        } else if (c.hasPath("schemaURI")) {
          Right(schemaURI)
        } else {
          Right(schema)
        }

        val stage = DebeziumTransformStage(
          plugin=this,
          id=id,
          name=name,
          description=description,
          inputView=inputView,
          outputView=outputView,
          schema=_schema,
          strict=strict,
        )

        if (c.hasPath("schemaView")) {
          stage.stageDetail.put("schemaView", c.getString("schemaView"))
        } else if (c.hasPath("schemaURI")) {
          stage.stageDetail.put("schemaURI", c.getString("schemaURI"))
        }
        authentication.foreach { authentication => stage.stageDetail.put("authentication", authentication.method) }
        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("outputView", outputView)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(id, name, description, inputView, outputView, schema, schemaURI, schemaView, authentication, strict, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class DebeziumTransformStage(
    plugin: DebeziumTransform,
    id: Option[String],
    name: String,
    description: Option[String],
    inputView: String,
    outputView: String,
    schema: Either[String, List[ExtractColumn]],
    strict: Boolean,
  ) extends TransformPipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    DebeziumTransformStage.execute(this)
  }
}

case class DebeziumStringKafkaEvent(
  key: Array[Byte],
  value: Array[Byte],
  topic: String,
  partition: Integer,
  offset: Long,
  timestamp: Timestamp,
  timestampType: Integer,
)

object DebeziumTransformStage {

  type DataEncoder = Row

  val OPERATION_CREATE = "c"
  val OPERATION_READ   = "r"
  val OPERATION_UPDATE = "u"
  val OPERATION_DELETE = "d"

  val CONNECTOR_MYSQL = "mysql"
  val CONNECTOR_MONGODB = "mongodb"
  val CONNECTOR_POSTGRESQL = "postgresql"

  val EVENT_KEY_INDEX = 0
  val EVENT_OFFSET_INDEX = 1
  val EVENT_CONNECTOR_INDEX = 2
  val EVENT_OPERATION_INDEX = 3
  val EVENT_BEFORE_INDEX = 4
  val EVENT_AFTER_INDEX = 5
  val EVENT_KEYMASK_INDEX = 6

  val EVENTS_KEY_INDEX = 0
  val EVENTS_EXISTS_INDEX = 1
  val EVENTS_EVENTS_INDEX = 2

  def execute(stage: DebeziumTransformStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._

    // try to get the schema
    val optionSchema = try {
      ExtractUtils.getSchema(stage.schema)(spark, logger)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    val df = spark.table(stage.inputView)

    if (!arcContext.isStreaming) {
      throw new Exception("DebeziumTransform can only be executed in streaming mode.") with DetailException {
        override val detail = stage.stageDetail
      }
    }
    if (!df.isStreaming) {
      throw new Exception("DebeziumTransform can only be executed against streaming sources.") with DetailException {
        override val detail = stage.stageDetail
      }
    }

    val cols = stage.schema match {
      case Right(cols) => {
        cols match {
          case Nil => throw new Exception(s"""DebeziumTransform requires an input schema to define how to transform data but the provided schema has 0 columns.""") with DetailException {
            override val detail = stage.stageDetail
          }
          case c => c
        }
      }
      case Left(view) => {
        val parseResult: ai.tripl.arc.util.ArcSchema.ParseResult = ai.tripl.arc.util.ArcSchema.parseArcSchemaDataFrame(spark.table(view))
        parseResult match {
          case Right(cols) => cols
          case Left(errors) => throw new Exception(s"""Schema view '${view}' to cannot be parsed as it has errors: ${errors.mkString(", ")}.""") with DetailException {
            override val detail = stage.stageDetail
          }
        }
      }
    }
    stage.stageDetail.put("columns", cols.map(_.name).asJava)

    val schema = Extract.toStructType(cols)
    val eventSchema = StructType(
      Seq(
        StructField("key", BinaryType, true),
        StructField("offset", LongType, true),
        StructField("connector", StringType, true),
        StructField("operation", StringType, true),
        StructField("before", schema, true),
        StructField("after", schema, true),
        StructField("keyMask", ArrayType(StringType, false), true),
      )
    )
    val eventsSchema = StructType(
      Seq(
        StructField("key", BinaryType, true),
        StructField("exists", BooleanType, true),
        StructField("events", ArrayType(eventSchema, true), true),
      )
    )

    val schemaEncoder = org.apache.spark.sql.catalyst.encoders.RowEncoder(schema)
    val eventEncoder = org.apache.spark.sql.catalyst.encoders.RowEncoder(eventSchema)
    val eventsEncoder = org.apache.spark.sql.catalyst.encoders.RowEncoder(eventsSchema)
    val emptyRowSeq = Seq.fill[Any](schema.length)(null)

    // rowFromStringObjectMap uses the supplied schema to try to read values from a Map[String,Object] produced by the ObjectMapper from a JSON string
    // connector is required to override some of the default behavior for different connectors
    // placeholder allows mongodb connector to return rows which respect field nullable rules - the keyMask will ignore those placeholder values in merge
    def rowFromStringObjectMap(afterMap: Map[String,Object], connector: String, placeholders: Boolean): Row = {
      Row.fromSeq(
        schema.fields.map { field =>
          field.dataType match {
            case BooleanType => {
              afterMap.get(field.name) match {
                case Some(v) => Try(v.asInstanceOf[Boolean]).getOrElse(v.asInstanceOf[String].toBoolean)
                case None => if (field.nullable) null else if (placeholders) false else throw new Exception(s"missing value for non-nullable field '${field.name}'")
              }
            }
            case DateType => {
              afterMap.get(field.name) match {
                case Some(v) => {
                  connector match {
                    case CONNECTOR_MONGODB => new Date(Instant.parse(v.asInstanceOf[Map[String,String]].get("$date").get).toEpochMilli)
                    case _ => new Date(v.asInstanceOf[Int].toLong * 86400000L)
                  }
                }
                case None => if (field.nullable) null else if (placeholders) new Date(0) else throw new Exception(s"missing value for non-nullable field '${field.name}'")
              }
            }
            case DecimalType() => {
              afterMap.get(field.name) match {
                case Some(v) => {
                  connector match {
                    case CONNECTOR_MONGODB => Decimal(v.asInstanceOf[Map[String,String]].get("$numberDecimal").get)
                    case _ => Decimal(v.asInstanceOf[String])
                  }
                }
                case None => if (field.nullable) null else if (placeholders) Decimal("0") else throw new Exception(s"missing value for non-nullable field '${field.name}'")
              }
            }
            case DoubleType => {
              afterMap.get(field.name) match {
                case Some(v) => Try(v.asInstanceOf[Double]).getOrElse(v.asInstanceOf[String].toDouble)
                case None => if (field.nullable) null else if (placeholders) 0 else throw new Exception(s"missing value for non-nullable field '${field.name}'")
              }
            }
            case IntegerType => {
              afterMap.get(field.name) match {
                case Some(v) => Try(v.asInstanceOf[Int]).getOrElse(v.asInstanceOf[String].toInt)
                case None => if (field.nullable) null else if (placeholders) 0 else throw new Exception(s"missing value for non-nullable field '${field.name}'")
              }
            }
            case LongType => {
              afterMap.get(field.name) match {
                case Some(v) => Try(Try(v.asInstanceOf[Long]).getOrElse(v.asInstanceOf[Int].toLong)).getOrElse(v.asInstanceOf[String].toLong)
                case None => if (field.nullable) null else if (placeholders) 0L else throw new Exception(s"missing value for non-nullable field '${field.name}'")
              }
            }
            case TimestampType => {
              afterMap.get(field.name) match {
                case Some(v) => {
                  connector match {
                    case CONNECTOR_MONGODB => new Timestamp(Instant.parse(v.asInstanceOf[Map[String,String]].get("$date").get).toEpochMilli)
                    case CONNECTOR_POSTGRESQL => new Timestamp(v.asInstanceOf[Long]/1000L)
                    case _ => new Timestamp(Instant.parse(v.asInstanceOf[String]).toEpochMilli)
                  }
                }
                case None => if (field.nullable) null else if (placeholders) new Timestamp(0) else throw new Exception(s"missing value for non-nullable field '${field.name}'")
              }
            }
            case StringType => {
              afterMap.get(field.name) match {
                case Some(v) => v.asInstanceOf[String]
                case None => if (field.nullable) null else if (placeholders) "" else throw new Exception(s"missing value for non-nullable field '${field.name}'")
              }
            }
            case NullType => null
            case _ => throw new Exception(s"unsupported type for field '${field.name}'")
          }
        }.toSeq
      )
    }

    // validateEvents takes a sequence of ordered events and iterates over them ensuring
    // the previous state is expected based on the OPERATION type
    def validateEvents(events: Seq[Row]): Row = {
      events.sliding(2).foreach { window =>
        if (window.length == 2) {
          val previous = window(0)
          val next = window(1)
          next.getString(EVENT_OPERATION_INDEX) match {
            case OPERATION_CREATE | OPERATION_READ => if (!previous.isNullAt(EVENT_AFTER_INDEX)) throw new Exception(s"expected previous value to be null for operation '${OPERATION_CREATE}'")
            case OPERATION_UPDATE => {
              if (previous.isNullAt(EVENT_AFTER_INDEX) || next.isNullAt(EVENT_BEFORE_INDEX) || previous.getStruct(EVENT_AFTER_INDEX) != next.getStruct(EVENT_BEFORE_INDEX)) {
                throw new Exception(s"expected previous value to equal next before value for operation '${OPERATION_UPDATE}'")
              }
            }
            case OPERATION_DELETE => {
              if (previous.isNullAt(EVENT_AFTER_INDEX) || next.isNullAt(EVENT_BEFORE_INDEX) || previous.getStruct(EVENT_AFTER_INDEX) != next.getStruct(EVENT_BEFORE_INDEX)) {
                throw new Exception(s"expected previous value to equal next before value for operation '${OPERATION_DELETE}'")
              }
            }
          }
        } else {
          val next = window(0)
          if (!Seq(OPERATION_CREATE, OPERATION_READ).contains(next.getString(EVENT_OPERATION_INDEX))) throw new Exception(s"expected previous value to be null for operations ['${OPERATION_CREATE}', '${OPERATION_READ}']")
        }
      }
      events.last
    }

    // applyMongoPatch takes a sequence of ordered events and applies the patches in sequence
    // it creates a Seq which is then updated in-place based on event type and uses the keyMask field to only update the impacted fields
    def applyMongoPatch(events: Seq[Row]): Row = {
      val patched = Row.fromSeq(
        events.drop(1).foldLeft(events.head.getStruct(EVENT_AFTER_INDEX).toSeq){ (accumulator, next) =>
          next.getString(EVENT_OPERATION_INDEX) match {
            case OPERATION_CREATE | OPERATION_READ => {
              if (accumulator != emptyRowSeq) throw new Exception(s"expected previous value to be null for operation '${OPERATION_CREATE}'")
              next.getStruct(EVENT_AFTER_INDEX).toSeq
            }
            case OPERATION_UPDATE => {
              if (accumulator == emptyRowSeq) throw new Exception(s"expected previous value to not be null for operation '${OPERATION_UPDATE}'")
              val keyMask = next.getSeq[String](EVENT_KEYMASK_INDEX)
              val patch = next.getStruct(EVENT_AFTER_INDEX)
              keyMask.foldLeft(accumulator){ (seq, key) =>
                seq.updated(schema.fieldIndex(key), patch.get(schema.fieldIndex(key)))
              }
            }
            case OPERATION_DELETE => {
              if (accumulator == emptyRowSeq) throw new Exception(s"expected previous value to not be null for operation '${OPERATION_DELETE}'")
              emptyRowSeq
            }
          }
        }
      )
      Row.fromSeq(events.last.toSeq.updated(EVENT_AFTER_INDEX, patched))
    }

    // todo: support avro
    val debeziumEvents = df
      .as[DebeziumStringKafkaEvent]
      // remove debezium tombstone events
      .filter { event => event.value != null }
      .mapPartitions { partition =>

        val objectMapper = new ObjectMapper()
        objectMapper.registerModule(DefaultScalaModule)

        var memoizedConnector: Option[String] = None

        partition.map { event =>
          if (event.key == null) throw new Exception("invalid configuration. expected 'key' to not be null. ensure primary key or connector 'message.key.columns' is set.")

          val valueMap = objectMapper.readValue(new String(event.value, StandardCharsets.UTF_8), classOf[Map[String,Map[String,Object]]])

          if (!valueMap.contains("payload")) throw new Exception(s"invalid message format. missing 'value.payload' attribute. got ${valueMap.keys.mkString("["," ,","]")}")
          val payload = valueMap.get("payload").getOrElse(throw new Exception("invalid message format. expected 'value.payload' to be Object."))

          if (!payload.contains("op")) throw new Exception(s"invalid message format. missing 'value.payload.op' attribute. got ${payload.keys.mkString("["," ,","]")}")
          val operation = Try(payload.get("op").get.asInstanceOf[String]).getOrElse(throw new Exception("invalid message format. expected 'value.payload.op' to be String."))

          val connector = memoizedConnector.getOrElse {
            if (!payload.contains("source")) throw new Exception(s"invalid message format. missing 'value.payload.source' attribute. got ${payload.keys.mkString("["," ,","]")}")
            val source = Try(payload.get("source").get.asInstanceOf[Map[String,Object]]).getOrElse(throw new Exception("invalid message format. expected 'value.payload.source' to be Object."))

            if (!source.contains("connector")) throw new Exception(s"invalid message format. missing 'value.payload.source.connector' attribute. got ${source.keys.mkString("["," ,","]")}")
            val connector = Try(source.get("connector").get.asInstanceOf[String]).getOrElse(throw new Exception("invalid message format. expected 'value.payload.source.connector' to be String."))

            if (connector == CONNECTOR_MONGODB && !stage.strict) throw new Exception(s"connector '${CONNECTOR_MONGODB}' requires strict mode.")

            memoizedConnector = Some(connector)
            memoizedConnector.get
          }

          val (before, after, keyMask) = connector match {
            case CONNECTOR_MYSQL | CONNECTOR_POSTGRESQL => {
              val before = if (stage.strict) {
                if (!payload.contains("before")) throw new Exception(s"invalid message format. missing 'value.payload.before' attribute. got ${payload.keys.mkString("["," ,","]")}")
                operation match {
                  case OPERATION_CREATE | OPERATION_READ => Try(payload.get("before").get.asInstanceOf[scala.Null]).getOrElse(throw new Exception(s"invalid message format. expected 'value.payload.before' to be null for operation '${OPERATION_CREATE}'."))
                  case OPERATION_UPDATE | OPERATION_DELETE => rowFromStringObjectMap(Try(payload.get("before").get.asInstanceOf[Map[String,Object]]).getOrElse(throw new Exception("invalid message format. expected 'value.payload.before' to be Object.")), connector, false)
                }
              } else {
                null
              }
              if (!payload.contains("after")) throw new Exception(s"invalid message format. missing 'value.payload.after' attribute. got ${payload.keys.mkString("["," ,","]")}")
              val after = operation match {
                case OPERATION_CREATE | OPERATION_READ | OPERATION_UPDATE => rowFromStringObjectMap(Try(payload.get("after").get.asInstanceOf[Map[String,Object]]).getOrElse(throw new Exception("invalid message format. expected 'value.payload.after' to be Object.")), connector, false)
                case OPERATION_DELETE => Try(payload.get("after").get.asInstanceOf[scala.Null]).getOrElse(throw new Exception(s"invalid message format. expected 'value.payload.after' to be null for operation '${OPERATION_DELETE}'."))
              }
              (before, after, null)
            }
            case CONNECTOR_MONGODB => {
              val before = null
              val keyMask = scala.collection.mutable.Set[String]()

              if (!payload.contains("after")) throw new Exception(s"invalid message format. missing 'value.payload.after' attribute. got ${payload.keys.mkString("["," ,","]")}")
              val after = operation match {
                case OPERATION_CREATE | OPERATION_READ => {
                  val after = Try(payload.get("after").get.asInstanceOf[String]).getOrElse(throw new Exception("invalid message format. expected 'value.payload.after' to be String."))
                  val createDocument = BsonDocument.parse(StringEscapeUtils.unescapeJson(after))
                  rowFromStringObjectMap(objectMapper.readValue(createDocument.toJson, classOf[Map[String,Object]]), connector, false)
                }
                case OPERATION_UPDATE => {
                  val patch = Try(payload.get("patch").get.asInstanceOf[String]).getOrElse(throw new Exception("invalid message format. expected 'value.payload.patch' to be String."))
                  val patchDocument = BsonDocument.parse(StringEscapeUtils.unescapeJson(patch))

                  var updateDocument = new BsonDocument()
                  if (patchDocument.containsKey("$set")) {
                    updateDocument = patchDocument.getDocument("$set")
                    keyMask ++= updateDocument.keySet.asScala
                  }
                  if (patchDocument.containsKey("$unset")) {
                    patchDocument.getDocument("$unset").entrySet.asScala.foreach { valueEntry =>
                      if (valueEntry.getValue.asBoolean.getValue) {
                        updateDocument.append(valueEntry.getKey(), new BsonNull)
                        keyMask += valueEntry.getKey
                      }
                    }
                  }
                  if (!patchDocument.containsKey("$set") && !patchDocument.containsKey("$unset")) {
                    if (!patchDocument.containsKey("_id")) throw new Exception("Unable to process Mongo Operation, a '$set' or '$unset' is necessary for partial updates or '_id' is expected for full Document replaces.")
                    // In case of a full update we can use the whole Document as it is
                    // see https://docs.mongodb.com/manual/reference/method/db.collection.update/#replace-a-document-entirely
                    updateDocument = patchDocument
                    updateDocument.remove("_id")
                  }

                  if (!patchDocument.containsKey("id")) {
                    val keyMap = objectMapper.readValue(new String(event.key, StandardCharsets.UTF_8), classOf[Map[String,Map[String,Object]]])

                    if (!keyMap.contains("payload")) throw new Exception(s"invalid message format. missing 'key.payload' attribute. got ${keyMap.keys.mkString("["," ,","]")}")
                    val payload = keyMap.get("payload").getOrElse(throw new Exception("invalid message format. expected 'key.payload' to be Object."))

                    if (!payload.contains("id")) throw new Exception(s"invalid message format. missing 'key.payload.id' attribute. got ${payload.keys.mkString("["," ,","]")}")
                    val id = Try(payload.get("id").get.asInstanceOf[String]).getOrElse(throw new Exception("invalid message format. expected 'key.payload.id' to be String."))
                    updateDocument.append("_id", new BsonString(id));
                  }

                  rowFromStringObjectMap(objectMapper.readValue(updateDocument.toJson, classOf[Map[String,Object]]), connector, true)
                }
                case OPERATION_DELETE => Try(payload.get("after").get.asInstanceOf[scala.Null]).getOrElse(throw new Exception(s"invalid message format. expected 'value.payload.after' to be null for operation '${OPERATION_DELETE}'."))
              }
              (before, after, keyMask.toSeq)
            }
            case default => throw new Exception(s"unsuppored connector '${default}'. expected one of ['${CONNECTOR_MONGODB}','${CONNECTOR_MYSQL}','${CONNECTOR_POSTGRESQL}'].")
          }

          Row(
            event.key,
            event.offset,
            connector,
            operation,
            before,
            after,
            keyMask,
          )
        }
      }(eventEncoder)

    val outputDF = if (stage.strict) {
      debeziumEvents
        .groupByKey(row => row.getAs[Array[Byte]](EVENT_KEY_INDEX))
        // mapGroups collects any events in the incoming batch and returns them as complete set
        // false indicates that the group has not been seen by reduceGroups
        .mapGroups((k, v) => Row(k, false, v.toSeq))(eventsEncoder)
        .groupByKey(row => row.getAs[Array[Byte]](EVENTS_KEY_INDEX))
        // reduceGroups
        // 'The given function must be commutative and associative or the result may be non-deterministic'
        // practically this means no order can be guaranteed
        // logically this works by:
        // 1. each executor reduces the rows that they manage and then send 0 or 1 records to the final reducer
        // 2. perform the final reduce.
        // this is good in that no executor needs to hold the entire change list in memory but is bad because order cannot be guaranteed
        //
        // reduceGroups also gets the existing value from the KV set unlike mapGroups
        // it will not be invoked if a previous key is not found
        // this strategy results in a last-write-wins but processes all events to ensure the state is changed correctly from one state to the next
        .reduceGroups((x: Row, y: Row) => {
          val events = (x.getSeq[Row](EVENTS_EVENTS_INDEX) ++ y.getSeq[Row](EVENTS_EVENTS_INDEX)).sortBy(event => event.getLong(EVENT_OFFSET_INDEX))

          val event = events.head.getString(EVENT_CONNECTOR_INDEX) match {
            case CONNECTOR_MYSQL | CONNECTOR_POSTGRESQL => validateEvents(events)
            case CONNECTOR_MONGODB => applyMongoPatch(events)
          }

          if (event.getString(EVENT_OPERATION_INDEX) == OPERATION_DELETE) {
            Row(event.getAs[Array[Byte]](EVENTS_KEY_INDEX), true, Seq.empty[Row])
          } else {
            Row(event.getAs[Array[Byte]](EVENTS_KEY_INDEX), true, Seq(event))
          }
        })
        // remove deletes
        .filter( !_._2.getSeq[Row](EVENTS_EVENTS_INDEX).isEmpty )
        .flatMap { case (_, e) => {
          if (e.getBoolean(EVENTS_EXISTS_INDEX)) {
            // events have been processed by reduceGroups
            Some(e.getSeq[Row](EVENTS_EVENTS_INDEX).last.getStruct(EVENT_AFTER_INDEX))
          } else {
            // events have not been processed by reduceGroups
            val events = e.getSeq[Row](EVENTS_EVENTS_INDEX).sortBy(event => event.getLong(EVENT_OFFSET_INDEX))

            val event = events.head.getString(EVENT_CONNECTOR_INDEX) match {
              case CONNECTOR_MYSQL | CONNECTOR_POSTGRESQL => validateEvents(events)
              case CONNECTOR_MONGODB => applyMongoPatch(events)
            }

            if (event.getString(EVENT_OPERATION_INDEX) == OPERATION_DELETE) {
              None
            } else {
              Some(event.getStruct(EVENT_AFTER_INDEX))
            }
          }
        }}(schemaEncoder)
    } else {
      debeziumEvents
        .groupByKey(row => row.getAs[Array[Byte]](EVENT_KEY_INDEX))
        // reduceGroups
        // 'The given function must be commutative and associative or the result may be non-deterministic'
        // practically this means no order can be guaranteed
        // logically this works by:
        // 1. each executor reduces the rows that they manage and then send 0 or 1 records to the final reducer
        // 2. perform the final reduce.
        // this is good in that no executor needs to hold the entire change list in memory but is bad because order cannot be guaranteed
        //
        // reduceGroups also gets the existing value from the KV set unlike mapGroups
        // this strategy is a simple last-write-wins
        .reduceGroups((x: Row, y: Row) => {
          if (x.getLong(EVENT_OFFSET_INDEX) > y.getLong(EVENT_OFFSET_INDEX)) x else y
        })
        // remove deletes
        .filter( !_._2.isNullAt(EVENT_AFTER_INDEX) )
        .map(_._2.getStruct(EVENT_AFTER_INDEX))(schemaEncoder)
    }

    if (arcContext.immutableViews) outputDF.createTempView(stage.outputView) else outputDF.createOrReplaceTempView(stage.outputView)
    Option(outputDF.toDF)
  }

}