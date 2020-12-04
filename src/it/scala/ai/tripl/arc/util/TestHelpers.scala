package ai.tripl.arc.util

import java.sql.Date

import org.apache.http.client.methods.{HttpPost, HttpDelete}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class Customer(
  c_custkey: Int,
  c_name: String,
  c_address: String,
  c_nationkey: Int,
  c_phone: String,
  c_acctbal: Decimal,
  c_mktsegment: String,
  c_comment: String,
)

case class Order(
  o_orderkey: Int,
  o_custkey: Int,
  o_orderstatus: String,
  o_totalprice: Decimal,
  o_orderdate: Date,
  o_orderpriority: String,
  o_clerk: String,
  o_shippriority: Int,
  o_comment: String,
)

object TestHelpers {

  def getTestCustomerData(filename: String, limit: Int = Int.MaxValue)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): (Dataset[Customer], Seq[Customer])= {
    import spark.implicits._

    val customerRaw = spark.read.format("csv").option("sep","|").load(spark.getClass.getResource(s"/${filename}").toString)
      .limit(limit)
      .select(
        col("_c0").cast("integer").as("c_custkey"),
        col("_c1").as("c_name"),
        col("_c2").as("c_address"),
        col("_c3").cast("integer").as("c_nationkey"),
        col("_c4").as("c_phone"),
        col("_c5").cast("decimal(20,2)").as("c_acctbal"),
        col("_c6").as("c_mktsegment"),
        col("_c7").as("c_comment"),
      ).as[Customer]
      .repartition(100)
    customerRaw.createOrReplaceTempView("customers")
    val count = customerRaw.cache.count
    val splitAt = (count * 0.6).toInt
    val customersInitial = customerRaw.where(s"c_custkey <= ${splitAt}")
    customersInitial.persist

    val customersUpdates = spark.sql(s"""
    SELECT
      c_custkey - ${(count-splitAt)/2} AS c_custkey
      ,c_name
      ,c_address
      ,c_nationkey
      ,c_phone
      ,c_acctbal
      ,c_mktsegment
      ,c_comment
    FROM customers
    WHERE c_custkey > ${splitAt}
    """).as[Customer].collect.toSeq

    (customersInitial, customersUpdates)
  }

  def getTestOrdersData(filename: String, limit: Int = Int.MaxValue)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): (Dataset[Order], Seq[Order])= {
    import spark.implicits._

    val orderRaw = spark.read.format("csv").option("sep","|").load(spark.getClass.getResource(s"/${filename}").toString)
      .limit(limit)
      .select(
        col("_c0").cast("integer").as("o_orderkey"),
        col("_c1").cast("integer").as("o_custkey"),
        col("_c2").as("o_orderstatus"),
        col("_c3").cast("decimal(20,2)").as("o_totalprice"),
        col("_c4").cast("date").as("o_orderdate"),
        col("_c5").as("o_orderpriority"),
        col("_c6").as("o_clerk"),
        col("_c7").cast("integer").as("o_shippriority"),
        col("_c8").as("o_comment"),
      ).as[Order]
      .repartition(100)
    orderRaw.createOrReplaceTempView("orders")
    val count = orderRaw.cache.count
    val splitAt = (count * 0.6).toInt
    val ordersInitial = orderRaw.where(s"o_orderkey <= ${splitAt}")
    ordersInitial.persist

    val ordersUpdates = spark.sql(s"""
    SELECT
      o_orderkey - ${(count-splitAt)/2} AS o_orderkey
      ,o_custkey
      ,o_orderstatus
      ,o_totalprice
      ,o_orderdate
      ,o_orderpriority
      ,o_clerk
      ,o_shippriority
      ,o_comment
    FROM orders
    WHERE o_orderkey > ${splitAt}
    """).as[Order].collect.toSeq

    (ordersInitial, ordersUpdates)
  }

  def registerConnector(uri: String, config: String) {
    // curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @register-mysql.json
    val entity = new StringEntity(config)

    val validStatusCode = Seq(201, 409)
    val client = HttpClients.createDefault
    try {
      val httpPost = new HttpPost(uri)
      httpPost.addHeader("Accept", "application/json")
      httpPost.addHeader("Content-Type", "application/json")
      httpPost.setEntity(entity)
      val response = client.execute(httpPost)
      try {
        val statusCode = response.getStatusLine.getStatusCode
        if (!validStatusCode.contains(statusCode)) {
          throw new Exception(s"""Expected StatusCode in ${validStatusCode.mkString("[", ", ", "]")} when POST '${uri}' but server responded with ${statusCode} (${response.getStatusLine.getReasonPhrase}).""")
        }
      } finally {
        response.close
      }
    } finally {
      client.close
    }
  }

  def deleteConnector(uri: String, connectorName: String) {
    // curl -i -X DELETE -H "Accept:application/json" -H  "Content-Type:application/json" http://connect:8083/connectors/inventory-connector
    val validStatusCode = Seq(204, 404)
    val client = HttpClients.createDefault
    try {
      val httpDelete = new HttpDelete(s"${uri}${connectorName}")
      httpDelete.addHeader("Accept", "application/json")
      httpDelete.addHeader("Content-Type", "application/json")
      val response = client.execute(httpDelete)
      try {
        val statusCode = response.getStatusLine.getStatusCode
        if (!validStatusCode.contains(statusCode)) {
          throw new Exception(s"""Expected StatusCode in ${validStatusCode.mkString("[", ", ", "]")} when DELETE '${uri}${connectorName}' but server responded with ${statusCode} (${response.getStatusLine.getReasonPhrase}).""")
        }
      } finally {
        response.close
      }
    } finally {
      client.close
    }
  }

}