package ai.tripl.arc.transform

import java.io._
import java.math.BigDecimal
import java.math.BigInteger
import java.net.URI
import java.nio.charset.StandardCharsets
import java.sql.Date
import java.sql.DriverManager
import java.sql.Timestamp
import java.sql.Timestamp
import java.time.Instant
import java.time.ZonedDateTime
import java.time.ZoneId
import java.util.Base64
import java.util.Properties

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

  def snippet()(implicit arcContext: ARCContext): String = {
    s"""{
    |  "type": "DebeziumTransform",
    |  "name": "DebeziumTransform",
    |  "environments": [${arcContext.completionEnvironments.map { env => s""""${env}""""}.mkString(", ")}],
    |  "inputView": "inputView",
    |  "outputView": "outputView",
    |  "schemaURI": "hdfs://*.json"
    |}""".stripMargin
  }

  val documentationURI = new java.net.URI(s"${baseURI}/transform/#debeziumtransform")

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "id" :: "name" :: "description" :: "environments" :: "inputView" :: "outputView" :: "authentication" :: "schema" :: "schemaURI" :: "schemaView" :: "strict" :: "initialStateView" :: "initialStateKey" :: "persist" :: "numPartitions" :: "partitionBy" :: Nil
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
    val initialStateView = getOptionalValue[String]("initialStateView")
    val initialStateKey = if (c.hasPath("initialStateView")) getValue[String]("initialStateKey") else Right("")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (id, name, description, inputView, outputView, schema, schemaURI, schemaView, authentication, strict, initialStateView, initialStateKey, persist, numPartitions, partitionBy, invalidKeys) match {
      case (Right(id), Right(name), Right(description), Right(inputView), Right(outputView), Right(schema), Right(schemaURI), Right(schemaView), Right(authentication), Right(strict), Right(initialStateView), Right(initialStateKey), Right(persist), Right(numPartitions), Right(partitionBy), Right(invalidKeys)) =>
        val _schema = if (c.hasPath("schemaView")) {
          Left(schemaView)
        } else if (c.hasPath("schemaURI")) {
          Right(schemaURI)
        } else {
          Right(schema)
        }
        val _initialStateKey = if (c.hasPath("initialStateView")) Option(initialStateKey) else None

        val stage = DebeziumTransformStage(
          plugin=this,
          id=id,
          name=name,
          description=description,
          inputView=inputView,
          outputView=outputView,
          schema=_schema,
          strict=strict,
          initialStateView=initialStateView,
          initialStateKey=_initialStateKey,
          persist=persist,
          numPartitions=numPartitions,
          partitionBy=partitionBy,
        )

        if (c.hasPath("schemaView")) {
          stage.stageDetail.put("schemaView", c.getString("schemaView"))
        } else if (c.hasPath("schemaURI")) {
          stage.stageDetail.put("schemaURI", c.getString("schemaURI"))
        }
        authentication.foreach { authentication => stage.stageDetail.put("authentication", authentication.method) }
        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("outputView", outputView)
        initialStateView.foreach { stage.stageDetail.put("initialStateView", _) }
        _initialStateKey.foreach { stage.stageDetail.put("initialStateKey", _) }
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))

        Right(stage)
      case _ =>
        val allErrors: Errors = List(id, name, description, inputView, outputView, schema, schemaURI, schemaView, authentication, strict, initialStateView, initialStateKey, persist, numPartitions, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
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
    initialStateView: Option[String],
    initialStateKey: Option[String],
    persist: Boolean,
    numPartitions: Option[Int],
    partitionBy: List[String],
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

  val CONNECTOR_STATE = "state"
  val CONNECTOR_MYSQL = "mysql"
  val CONNECTOR_MONGODB = "mongodb"
  val CONNECTOR_POSTGRESQL = "postgresql"
  val CONNECTOR_ORACLE = "oracle"

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

    val enrichedCols = cols ++ List(
      StringColumn(None, name="_topic", description=Some("An Arc internal field describing where this row was originally sourced from."), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil,  metadata=Some("""{"internal": true}"""), minLength=None, maxLength=None, regex=None),
      LongColumn(None, name="_offset", description=Some("An Arc internal field describing the offset in _topic this row was originally sourced from."), nullable=false, nullReplacementValue=None, trim=false, nullableValues="" :: Nil,  metadata=Some("""{"internal": true}"""), formatters=None)
    )

    val schema = Extract.toStructType(enrichedCols)
    val caseSensitiveSchema = schema.fields.exists(field => field.name.toLowerCase != field.name )
    val eventSchema = StructType(
      Seq(
        StructField("key", StringType, true),
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
        StructField("key", StringType, true),
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
    def rowFromStringObjectMap(schemaList: List[Map[String,Object]], dataMap: Map[String,Object], connector: String, topic: String, offset: Long, placeholders: Boolean): Row = {

      // postgres does not support case sensitive column names
      def casedFieldName(name: String): String = {
        if (caseSensitiveSchema && connector == CONNECTOR_POSTGRESQL) name.toLowerCase else name
      }

      Row.fromSeq(
        enrichedCols.map { field =>
          if (field.name == "_topic") {
            topic
          } else if (field.name == "_offset") {
            offset
          } else {
            val schemaField = connector match {
              case CONNECTOR_MONGODB => Map.empty[String,String]
              case CONNECTOR_POSTGRESQL if (caseSensitiveSchema) => Try(schemaList.filter { schemaField => schemaField.get("field") == Some(field.name.toLowerCase) }.head).getOrElse(throw new Exception(s"missing schema for field '${field.name}'"))
              case _ => Try(schemaList.filter { schemaField => schemaField.get("field") == Some(field.name) }.head).getOrElse(throw new Exception(s"missing schema for field '${field.name}'"))
            }
            val fieldType = connector match {
              case CONNECTOR_MONGODB => ""
              case _ => Try(schemaField.get("type").get.asInstanceOf[String]).getOrElse(throw new Exception(s"expected 'type' schema for field '${field.name}' to be String."))
            }
            val fieldTypeName = connector match {
              case CONNECTOR_MONGODB => ""
              case _ => Try(schemaField.get("name").map(_.asInstanceOf[String])).getOrElse(throw new Exception(s"expected 'name' schema for field '${field.name}' to be String."))
            }

            field match {
              case _: BooleanColumn => {
                dataMap.get(casedFieldName(field.name)) match {
                  case Some(v) => {
                    v match {
                      case b: java.lang.Boolean => b
                      case i: java.lang.Integer => i != 0
                      case s: String => s.toBoolean
                      case n if n == null => if (field.nullable) null else if (placeholders) "" else throw new Exception(s"missing value for non-nullable field '${field.name}'")
                      case _ => throw new Exception(s"'${v.getClass.getName}' does not match expected data type '${field.sparkDataType.simpleString}' for field '${field.name}'.")
                    }
                  }
                  case None => if (field.nullable) null else if (placeholders) false else throw new Exception(s"missing value for non-nullable field '${field.name}'")
                }
              }
              case  _: DateColumn => {
                dataMap.get(casedFieldName(field.name)) match {
                  case Some(v) => {
                    v match {
                      case i: java.lang.Integer => new Date(i.toLong * 86400000L)
                      case l: java.lang.Long => new Date(l * 86400000L)
                      case m: Map[_,_] => new Date(Instant.parse(v.asInstanceOf[Map[String,String]].get("$date").get).toEpochMilli)
                      case n if n == null => if (field.nullable) null else if (placeholders) "" else throw new Exception(s"missing value for non-nullable field '${field.name}'")
                      case _ => throw new Exception(s"'${v.getClass.getName}' does not match expected data type '${field.sparkDataType.simpleString}' for field '${field.name}'.")
                    }
                  }
                  case None => if (field.nullable) null else if (placeholders) new Date(0) else throw new Exception(s"missing value for non-nullable field '${field.name}'")
                }
              }
              case  _: DecimalColumn => {
                dataMap.get(casedFieldName(field.name)) match {
                  case Some(v) => {
                    v match {
                      case s: String => {
                        fieldType match {
                          case "bytes" => {
                            val parametersMap = Try(schemaField.get("parameters").get.asInstanceOf[Map[String,String]]).getOrElse(throw new Exception(s"expected 'parameters' schema for field '${field.name}' of type 'bytes' to be Map[String, String]."))
                            val scale = Try(parametersMap.get("scale").get.toInt).getOrElse(0)
                            val precision = Try(parametersMap.get("connect.decimal.precision").get.toInt).getOrElse(38)
                            val bytes = Base64.getDecoder().decode(v.asInstanceOf[String])
                            val bigDecimal = scala.math.BigDecimal(new BigInteger(bytes))
                            Decimal(bigDecimal / scala.math.pow(10, scale), precision, scale)
                          }
                          case "string" => Decimal(v.asInstanceOf[String])
                          case _ => throw new Exception(s"expected source data type as one of ['bytes', 'string'] for field '${field.name}' of type Decimal")
                        }
                      }
                      case m: Map[_,_] => Decimal(v.asInstanceOf[Map[String,String]].get("$numberDecimal").get)
                      case n if n == null => if (field.nullable) null else if (placeholders) "" else throw new Exception(s"missing value for non-nullable field '${field.name}'")
                      case _ => throw new Exception(s"'${v.getClass.getName}' does not match expected data type '${field.sparkDataType.simpleString}' for field '${field.name}'.")
                    }
                  }
                  case None => if (field.nullable) null else if (placeholders) Decimal("0") else throw new Exception(s"missing value for non-nullable field '${field.name}'")
                }
              }
              case  _: DoubleColumn => {
                dataMap.get(casedFieldName(field.name)) match {
                  case Some(v) => {
                    v match {
                      case d: java.lang.Double => d
                      case f: java.lang.Float => f.toDouble
                      case s: String => s.toDouble
                      case n if n == null => if (field.nullable) null else if (placeholders) "" else throw new Exception(s"missing value for non-nullable field '${field.name}'")
                      case _ => throw new Exception(s"'${v.getClass.getName}' does not match expected data type '${field.sparkDataType.simpleString}' for field '${field.name}'.")
                    }
                  }
                  case None => if (field.nullable) null else if (placeholders) 0 else throw new Exception(s"missing value for non-nullable field '${field.name}'")
                }
              }
              case  _: IntegerColumn => {
                dataMap.get(casedFieldName(field.name)) match {
                  case Some(v) => {
                    v match {
                      case i: java.lang.Integer => i
                      case l: java.lang.Long => l.toInt
                      case s: String => s.toInt
                      case n if n == null => if (field.nullable) null else if (placeholders) "" else throw new Exception(s"missing value for non-nullable field '${field.name}'")
                      case _ => throw new Exception(s"'${v.getClass.getName}' does not match expected data type '${field.sparkDataType.simpleString}' for field '${field.name}'.")
                    }
                  }
                  case None => if (field.nullable) null else if (placeholders) 0 else throw new Exception(s"missing value for non-nullable field '${field.name}'")
                }
              }
              case  _: LongColumn => {
                dataMap.get(casedFieldName(field.name)) match {
                  case Some(v) => {
                    v match {
                      case l: java.lang.Long => l
                      case i: java.lang.Integer => i.toLong
                      case s: String => s.toLong
                      case n if n == null => if (field.nullable) null else if (placeholders) "" else throw new Exception(s"missing value for non-nullable field '${field.name}'")
                      case _ => throw new Exception(s"'${v.getClass.getName}' does not match expected data type '${field.sparkDataType.simpleString}' for field '${field.name}'.")
                    }
                  }
                  case None => if (field.nullable) null else if (placeholders) 0L else throw new Exception(s"missing value for non-nullable field '${field.name}'")
                }
              }
              case t: TimestampColumn => {
                dataMap.get(casedFieldName(field.name)) match {
                  case Some(v) => {

                    connector match {
                      case CONNECTOR_MONGODB => {
                        v match {
                          case m: Map[_,_] => new Timestamp(Instant.parse(m.asInstanceOf[Map[String,String]].get("$date").get).toEpochMilli)
                          case n if n == null => if (field.nullable) null else if (placeholders) "" else throw new Exception(s"missing value for non-nullable field '${field.name}'")
                          case _ => throw new Exception(s"'${v.getClass.getName}' does not match expected data type '${field.sparkDataType.simpleString}' for field '${field.name}'.")
                        }
                      }
                      case _ => {
                        v match {
                          case l: java.lang.Long => {
                            fieldTypeName match {
                              case Some("io.debezium.time.Timestamp") => new Timestamp(ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneId.of(t.timezoneId)).plusNanos(l*1000000).toInstant.toEpochMilli)
                              case Some("io.debezium.time.MicroTimestamp") => new Timestamp(l/1000)
                              case Some("io.debezium.time.ZonedTimestamp") => new Timestamp(l/1000)
                              case None => throw new Exception(s"expected 'name' schema for field '${field.name}' to be String but was not provided.")
                            }
                          }
                          case i: java.lang.Integer => {
                            fieldTypeName match {
                              case Some("io.debezium.time.Timestamp") => new Timestamp(ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneId.of(t.timezoneId)).plusNanos(i.toLong*1000000).toInstant.toEpochMilli)
                              case Some("io.debezium.time.MicroTimestamp") => new Timestamp(i.toLong/1000)
                              case Some("io.debezium.time.ZonedTimestamp") => new Timestamp(i.toLong/1000)
                              case None => throw new Exception(s"expected 'name' schema for field '${field.name}' to be String but was not provided.")
                              case _ =>
                            }
                          }
                          case s: String => new Timestamp(Instant.parse(s).toEpochMilli)
                          case n if n == null => if (field.nullable) null else if (placeholders) "" else throw new Exception(s"missing value for non-nullable field '${field.name}'")
                          case _ => throw new Exception(s"'${v.getClass.getName}' does not match expected data type '${field.sparkDataType.simpleString}' for field '${field.name}'.")
                        }
                      }
                    }


                  }
                  case None => if (field.nullable) null else if (placeholders) new Timestamp(0) else throw new Exception(s"missing value for non-nullable field '${field.name}'")
                }
              }
              case  _: StringColumn  => {
                dataMap.get(casedFieldName(field.name)) match {
                  case Some(v) => {
                    v match {
                      case s: String => s
                      case n if n == null => if (field.nullable) null else if (placeholders) "" else throw new Exception(s"missing value for non-nullable field '${field.name}'")
                      case _ => throw new Exception(s"'${v.getClass.getName}' does not match expected data type '${field.sparkDataType.simpleString}' for field '${field.name}'.")
                    }
                  }
                  case None => if (field.nullable) null else if (placeholders) "" else throw new Exception(s"missing value for non-nullable field '${field.name}'")
                }
              }
              case  _: TimeColumn  => {
                dataMap.get(casedFieldName(field.name)) match {
                  case Some(v) => {
                    v match {
                      case s: String => s
                      case n if n == null => if (field.nullable) null else if (placeholders) "" else throw new Exception(s"missing value for non-nullable field '${field.name}'")
                      case _ => throw new Exception(s"'${v.getClass.getName}' does not match expected data type '${field.sparkDataType.simpleString}' for field '${field.name}'.")
                    }
                  }
                  case None => if (field.nullable) null else if (placeholders) "" else throw new Exception(s"missing value for non-nullable field '${field.name}'")
                }
              }
              case _ => throw new Exception(s"unsupported type for field '${field.name}'")
            }
          }
        }
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
              if (previous.isNullAt(EVENT_AFTER_INDEX) || next.isNullAt(EVENT_BEFORE_INDEX) || previous.getStruct(EVENT_AFTER_INDEX).toSeq.dropRight(1) != next.getStruct(EVENT_BEFORE_INDEX).toSeq.dropRight(1)) {
                throw new Exception(s"expected previous value to equal next before value for operation '${OPERATION_UPDATE}'")
              }
            }
            case OPERATION_DELETE => {
              if (previous.isNullAt(EVENT_AFTER_INDEX) || next.isNullAt(EVENT_BEFORE_INDEX) || previous.getStruct(EVENT_AFTER_INDEX).toSeq.dropRight(1) != next.getStruct(EVENT_BEFORE_INDEX).toSeq.dropRight(1)) {
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

    // map the events to change rows
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
          val keyString = new String(event.key, StandardCharsets.UTF_8)
          val keyMap = objectMapper.readValue(keyString, classOf[Map[String,Map[String,String]]])

          if (!keyMap.contains("payload")) throw new Exception(s"invalid message format. missing 'key.payload' attribute. got ${keyMap.keys.mkString("["," ,","]")}")
          val keyPayload = keyMap.get("payload").getOrElse(throw new Exception("invalid message format. expected 'key.payload' to be Object."))
          // this currently only supports 1:1 mapping of key when used with initialStateKey
          val key = keyPayload.values.mkString("|")

          val valueString = new String(event.value, StandardCharsets.UTF_8)
          val valueMap = objectMapper.readValue(valueString, classOf[Map[String,Map[String,Object]]])

          if (!valueMap.contains("payload")) throw new Exception(s"invalid message format. missing 'value.payload' attribute. got ${valueMap.keys.mkString("["," ,","]")}")
          val valuePayload = valueMap.get("payload").getOrElse(throw new Exception("invalid message format. expected 'value.payload' to be Object."))

          val connector = memoizedConnector.getOrElse {
            if (!valuePayload.contains("source")) throw new Exception(s"invalid message format. missing 'value.payload.source' attribute. got ${valuePayload.keys.mkString("["," ,","]")}")
            val source = Try(valuePayload.get("source").get.asInstanceOf[Map[String,Object]]).getOrElse(throw new Exception("invalid message format. expected 'value.payload.source' to be Object."))

            if (!source.contains("connector")) throw new Exception(s"invalid message format. missing 'value.payload.source.connector' attribute. got ${source.keys.mkString("["," ,","]")}")
            val connector = Try(source.get("connector").get.asInstanceOf[String]).getOrElse(throw new Exception("invalid message format. expected 'value.payload.source.connector' to be String."))

            if (connector == CONNECTOR_MONGODB && !stage.strict) throw new Exception(s"connector '${CONNECTOR_MONGODB}' requires strict mode.")

            memoizedConnector = Some(connector)
            memoizedConnector.get
          }

          if (!valueMap.contains("schema")) throw new Exception(s"invalid message format. missing 'value.schema' attribute. got ${valueMap.keys.mkString("["," ,","]")}")
          val schemaPayload = valueMap.get("schema").getOrElse(throw new Exception("invalid message format. expected 'value.schema' to be Object."))
          val fields = Try(schemaPayload.get("fields").get.asInstanceOf[List[Map[String,Object]]]).getOrElse(throw new Exception("invalid message format. expected 'value.schema.fields' to be Array."))
          val afterFields = if (connector == CONNECTOR_MONGODB) List.empty else Try(fields.filter { fieldMap => fieldMap.get("field") == Some("after") }.head.get("fields").get.asInstanceOf[List[Map[String, Object]]]).getOrElse(throw new Exception("invalid message format. expected 'value.schema.fields.after' to be Array."))

          if (!valuePayload.contains("op")) throw new Exception(s"invalid message format. missing 'value.payload.op' attribute. got ${valuePayload.keys.mkString("["," ,","]")}")
          val operation = Try(valuePayload.get("op").get.asInstanceOf[String]).getOrElse(throw new Exception("invalid message format. expected 'value.payload.op' to be String."))

          val (before, after, keyMask) = connector match {
            case CONNECTOR_MYSQL | CONNECTOR_POSTGRESQL | CONNECTOR_ORACLE => {
              val before = if (stage.strict) {
                if (!valuePayload.contains("before")) throw new Exception(s"invalid message format. missing 'value.payload.before' attribute. got ${valuePayload.keys.mkString("["," ,","]")}")
                val beforeFields = Try(fields.filter { fieldMap => fieldMap.get("field") == Some("before") }.head.get("fields").get.asInstanceOf[List[Map[String, Object]]]).getOrElse(throw new Exception("invalid message format. expected 'value.schema.fields.before' to be Array."))
                operation match {
                  case OPERATION_CREATE | OPERATION_READ => Try(valuePayload.get("before").get.asInstanceOf[scala.Null]).getOrElse(throw new Exception(s"invalid message format. expected 'value.payload.before' to be null for operation '${OPERATION_CREATE}'."))
                  case OPERATION_UPDATE | OPERATION_DELETE => rowFromStringObjectMap(beforeFields, Try(valuePayload.get("before").get.asInstanceOf[Map[String,Object]]).getOrElse(throw new Exception("invalid message format. expected 'value.payload.before' to be Object.")), connector, event.topic, event.offset, false)
                }
              } else {
                null
              }
              if (!valuePayload.contains("after")) throw new Exception(s"invalid message format. missing 'value.payload.after' attribute. got ${valuePayload.keys.mkString("["," ,","]")}")
              val after = operation match {
                case OPERATION_CREATE | OPERATION_READ | OPERATION_UPDATE => rowFromStringObjectMap(afterFields, Try(valuePayload.get("after").get.asInstanceOf[Map[String,Object]]).getOrElse(throw new Exception("invalid message format. expected 'value.payload.after' to be Object.")), connector, event.topic, event.offset, false)
                case OPERATION_DELETE => Try(valuePayload.get("after").get.asInstanceOf[scala.Null]).getOrElse(throw new Exception(s"invalid message format. expected 'value.payload.after' to be null for operation '${OPERATION_DELETE}'."))
              }
              (before, after, null)
            }
            case CONNECTOR_MONGODB => {
              val before = null
              val keyMask = scala.collection.mutable.Set[String]()

              if (!valuePayload.contains("after")) throw new Exception(s"invalid message format. missing 'value.payload.after' attribute. got ${valuePayload.keys.mkString("["," ,","]")}")
              val after = operation match {
                case OPERATION_CREATE | OPERATION_READ => {
                  val after = Try(valuePayload.get("after").get.asInstanceOf[String]).getOrElse(throw new Exception("invalid message format. expected 'value.payload.after' to be String."))
                  val createDocument = BsonDocument.parse(StringEscapeUtils.unescapeJson(after))
                  rowFromStringObjectMap(afterFields, objectMapper.readValue(createDocument.toJson, classOf[Map[String,Object]]), connector, event.topic, event.offset, false)
                }
                case OPERATION_UPDATE => {
                  val patch = Try(valuePayload.get("patch").get.asInstanceOf[String]).getOrElse(throw new Exception("invalid message format. expected 'value.payload.patch' to be String."))
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
                    if (!keyPayload.contains("id")) throw new Exception(s"invalid message format. missing 'key.payload.id' attribute. got ${keyPayload.keys.mkString("["," ,","]")}")
                    val id = Try(keyPayload.get("id").get).getOrElse(throw new Exception("invalid message format. expected 'key.payload.id' to be String."))
                    updateDocument.append("_id", new BsonString(id));
                  }

                  rowFromStringObjectMap(afterFields, objectMapper.readValue(updateDocument.toJson, classOf[Map[String,Object]]), connector, event.topic, event.offset, true)
                }
                case OPERATION_DELETE => Try(valuePayload.get("after").get.asInstanceOf[scala.Null]).getOrElse(throw new Exception(s"invalid message format. expected 'value.payload.after' to be null for operation '${OPERATION_DELETE}'."))
              }
              (before, after, keyMask.toSeq)
            }
            case default => throw new Exception(s"unsupported connector '${default}'. expected one of ['${CONNECTOR_MONGODB}','${CONNECTOR_MYSQL}','${CONNECTOR_POSTGRESQL}', '${CONNECTOR_ORACLE}'].")
          }

          Row(
            key,
            event.offset,
            connector,
            operation,
            before,
            after,
            keyMask,
          )
        }
      }(eventEncoder)
      .groupByKey { row => row.getString(EVENT_KEY_INDEX) }


    // if previous state is provided inject here
    val statefulDF = stage.initialStateView.map { initialStateView =>
      // this group by key needs to be written to map values properly
      val groupedInitialStateView = spark.table(initialStateView).groupByKey { row => row.get(row.fieldIndex(stage.initialStateKey.get)).toString }
      debeziumEvents.cogroup(groupedInitialStateView) { case (key, debeziumEvents, initialState) =>
        initialState.map { row =>
          Row.fromSeq(
            Seq(
              key,
              0L,
              CONNECTOR_STATE,
              OPERATION_READ,
              null,
              row,
              Seq.empty[String]
            )
          )
        } ++
        debeziumEvents
      }(eventEncoder)
      .groupByKey { row => row.getString(EVENT_KEY_INDEX) }
    }.getOrElse(debeziumEvents)

    // perform the merge
    val outputDF = if (stage.strict) {
      statefulDF
        // mapGroups collects any events in the incoming batch and returns them as complete set
        // false indicates that the group has not been seen by reduceGroups
        .mapGroups { (k, v) => Row(k, false, v.toSeq) }(eventsEncoder)
        .groupByKey { row => row.getString(EVENTS_KEY_INDEX) }
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
        .reduceGroups {
          (x: Row, y: Row) => {
            val events = (x.getSeq[Row](EVENTS_EVENTS_INDEX) ++ y.getSeq[Row](EVENTS_EVENTS_INDEX)).sortBy(event => event.getLong(EVENT_OFFSET_INDEX))

            val event = events.head.getString(EVENT_CONNECTOR_INDEX) match {
              case CONNECTOR_MYSQL | CONNECTOR_POSTGRESQL | CONNECTOR_ORACLE | CONNECTOR_STATE => validateEvents(events)
              case CONNECTOR_MONGODB => applyMongoPatch(events)
            }

            if (event.getString(EVENT_OPERATION_INDEX) == OPERATION_DELETE) {
              Row(event.getString(EVENTS_KEY_INDEX), true, Seq.empty[Row])
            } else {
              Row(event.getString(EVENTS_KEY_INDEX), true, Seq(event))
            }
          }
        }
        // remove deletes
        .filter { !_._2.getSeq[Row](EVENTS_EVENTS_INDEX).isEmpty }
        .flatMap {
          case (_, e) => {
            if (e.getBoolean(EVENTS_EXISTS_INDEX)) {
              // events have been processed by reduceGroups
              Some(e.getSeq[Row](EVENTS_EVENTS_INDEX).last.getStruct(EVENT_AFTER_INDEX))
            } else {
              // events have not been processed by reduceGroups
              val events = e.getSeq[Row](EVENTS_EVENTS_INDEX).sortBy(event => event.getLong(EVENT_OFFSET_INDEX))

              val event = events.head.getString(EVENT_CONNECTOR_INDEX) match {
                case CONNECTOR_MYSQL | CONNECTOR_POSTGRESQL | CONNECTOR_ORACLE | CONNECTOR_STATE=> validateEvents(events)
                case CONNECTOR_MONGODB => applyMongoPatch(events)
              }

              if (event.getString(EVENT_OPERATION_INDEX) == OPERATION_DELETE) {
                None
              } else {
                Some(event.getStruct(EVENT_AFTER_INDEX))
              }
            }
          }
        }(schemaEncoder)
    } else {
      statefulDF
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
        .reduceGroups {
          (x: Row, y: Row) => {
            if (x.getLong(EVENT_OFFSET_INDEX) > y.getLong(EVENT_OFFSET_INDEX)) x else y
          }
        }
        // remove deletes
        .filter { !_._2.isNullAt(EVENT_AFTER_INDEX) }
        .map { _._2.getStruct(EVENT_AFTER_INDEX) }(schemaEncoder)
    }

    // set column metadata if exists
    val enrichedDF = optionSchema match {
        case Some(schema) => MetadataUtils.setMetadata(outputDF, schema)
        case None => outputDF
    }

    // repartition to distribute rows evenly
    val repartitionedDF = stage.partitionBy match {
      case Nil => {
        stage.numPartitions match {
          case Some(numPartitions) => enrichedDF.repartition(numPartitions)
          case None => enrichedDF
        }
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => enrichedDF(col))
        stage.numPartitions match {
          case Some(numPartitions) => enrichedDF.repartition(numPartitions, partitionCols:_*)
          case None => enrichedDF.repartition(partitionCols:_*)
        }
      }
    }

    if (arcContext.immutableViews) repartitionedDF.createTempView(stage.outputView) else repartitionedDF.createOrReplaceTempView(stage.outputView)

    if (!repartitionedDF.isStreaming) {
      stage.stageDetail.put("outputColumns", Integer.valueOf(repartitionedDF.schema.length))
      stage.stageDetail.put("numPartitions", Integer.valueOf(repartitionedDF.rdd.partitions.length))

      if (stage.persist) {
        spark.catalog.cacheTable(stage.outputView, arcContext.storageLevel)
        stage.stageDetail.put("records", java.lang.Long.valueOf(repartitionedDF.count))
      }
    }

    Option(repartitionedDF.toDF)
  }

}