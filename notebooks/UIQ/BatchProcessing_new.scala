// Databricks notebook source
// import needed for the .avro() method to become available
import com.databricks.spark.avro._
import org.apache.spark.sql.functions._

import org.joda.time._
import org.joda.time.format._

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config

import org.apache.spark.sql.functions._
import org.joda.time._
import org.joda.time.format._
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config
import org.apache.spark.sql.functions._

import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql.functions._
import spark.implicits._
import org.apache.spark.sql.types._
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config
import org.joda.time._
import org.joda.time.format._
import com.microsoft.azure.cosmosdb.spark.streaming.CosmosDBSinkProvider
// The Avro records get converted to Spark types, filtered, and
// then written back out as Avro records
val df = spark.read.avro("dbfs:/mnt/jll_cap/2018/10/16/*.avro")

var j = 
  df
  .select(from_json(($"body").cast("string"), schema).alias("a"))
  .select($"a.*") // TODO: Improve schema to not need "a"

display(j)
/*
var j = 
  df
   .select(get_json_object(($"body").cast("string"), "$.Space").alias("Space"), get_json_object(($"body").cast("string"), "$.Datetime5Minutes").alias("Datetime5Minutes"))
*/
   // .groupBy($"Space", window($"Datetime5Minutes".cast("timestamp"), "1 hour")) 
  //  .count()

/*
var x = j.select($"Space").filter($"Datetime5Minutes".cast("timestamp") > "2018-04-27 13:50:00")
var y = j.select($"Space").filter($"Datetime5Minutes".cast("timestamp") > "2018-04-27 13:50:00")
display(x.select("Space"))
*/


/*
val df = eventhubs
  .select(from_json(($"body").cast("string"), schema).alias("a"))
  .select($"a.*") // TODO: Improve schema to not need "a"
  .withColumn("window", window($"Datetime5Minutes", windowDuration, slideDuration))
*/






// COMMAND ----------

// MAGIC %python
// MAGIC avroDf = spark.read.format("com.databricks.spark.avro").load("dbfs:/mnt/jll_cap/2018/10/16/*.avro")
// MAGIC jsonRdd = avroDf.select(avroDf.Body.cast("string")).rdd.map(lambda x: x[0])
// MAGIC data = spark.read.json(jsonRdd)
// MAGIC data.createOrReplaceTempView("events")
// MAGIC                                                              

// COMMAND ----------

// MAGIC %python
// MAGIC data.coalesce(1).write.parquet("dbfs:/mnt/jll_cap/test2.parquet")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from events where type = 'TRAFFIC' and areaId = 'ATXW9j1aQBmvn5cTgekezQ';

// COMMAND ----------

// MAGIC %python
// MAGIC display(data)

// COMMAND ----------

import com.databricks.spark.avro._

// The Avro records get converted to Spark types, filtered, and
// then written back out as Avro records
val df = spark.read.avro("dbfs:/mnt/jll_cap/2018/10/16/*.avro")

var j = 
  df
  .select(from_json(($"body").cast("string"), schema).alias("a"))
  .select($"a.*") // TODO: Improve schema to not need "a"

display(j)

// COMMAND ----------


val rollupQuery  = """select Space,  datetime5Minutes, sum(NumeratorUtilization) sum_NumeratorUtilization, 1 as DenominatorUtilization, sum(NumeratorOccupancy) sum_NumeratorOccupancy, sum(DenominatorOccupancy) sum_DenominatorOccupancy
, avg(AmbientTemp) avg_AmbientTemp

from rawdata

group by space, datetime5Minutes"""

val rollupDF = 
spark.sql(rollupQuery)

rollupDF.count

rollupDF.write
  .format("com.databricks.spark.csv")
  .option("header", "true")
  .save("/mnt/utilizationiqsensor/utilizationiqsensor/output/rollupDFExport.CSV")




// COMMAND ----------



val rollupRead = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/mnt/utilizationiqsensor/utilizationiqsensor/output/rollupDFExport.CSV")

display(rollupRead)



// COMMAND ----------




rollupRead
  .coalesce(1)
  .write
  .format("com.databricks.spark.csv")
  .option("header", "true")
  .save("/mnt/utilizationiqsensor/utilizationiqsensor/output/rollupDFExport_flat.CSV")



