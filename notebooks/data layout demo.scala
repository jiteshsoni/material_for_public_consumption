// Databricks notebook source
// MAGIC %md
// MAGIC ## What is the demo about ?
// MAGIC 
// MAGIC #### Show performance difference between
// MAGIC 1.) compacting small files (optimize)    
// MAGIC 2.) Compatcing the files and  sorting/colocating the data in files ( Optimize and Z-order )    
// MAGIC 3.) Partition the data and Compatcing the files and  sorting/colocating the data in files ( Partitioning and Optimize and Z-order )

// COMMAND ----------

// MAGIC %md
// MAGIC ## Data Layout Demo

// COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", false)

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE WIDGET TEXT delta_table_path DEFAULT "/tmp/optimize_zorder_demo/"

// COMMAND ----------

// MAGIC %md
// MAGIC ### Generate data

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions._
// MAGIC for (part <- 1 to 100) {
// MAGIC   for (fileNum <- 1 to 1000) {
// MAGIC     val total = 500000
// MAGIC     spark.range(total)
// MAGIC       .withColumn("part", lit(part))
// MAGIC       .withColumn("date", current_date())
// MAGIC       .withColumn("frequency1", $"id" % total)
// MAGIC       .withColumn("frequency10", $"id" % (total / 10))
// MAGIC       .withColumn("frequency100", $"id" % (total / 100))
// MAGIC       .withColumn("frequency1000", $"id" % (total / 1000))
// MAGIC       .withColumn("frequency10000", $"id" % (total/ 10000))
// MAGIC       .withColumn("frequency100000", $"id" % (total / 100000))
// MAGIC       .repartition(1)
// MAGIC       .write
// MAGIC       .format("delta")
// MAGIC       .mode("append")
// MAGIC       .partitionBy("part")
// MAGIC       .save(s"${delta_table_path}")
// MAGIC   }
// MAGIC }

// COMMAND ----------

// MAGIC %sql
// MAGIC DESC DETAIL delta.`${delta_table_path}`

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE TABLE EXTENDED  delta.`${delta_table_path}`

// COMMAND ----------

// MAGIC %sql
// MAGIC Select count(*) from delta.`${delta_table_path}`
// MAGIC --8,023,000,000

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM delta.`${delta_table_path}` WHERE frequency1=1

// COMMAND ----------

// MAGIC %md
// MAGIC ## Create clones of the table (just so that experiments are easier)

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TABLE nonZorderButPartitioned SHALLOW CLONE delta.`${delta_table_path}`

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TABLE zorderDefaultButPartitioned SHALLOW CLONE nonZorderButPartitioned

// COMMAND ----------

// MAGIC %md
// MAGIC ### OPTIMIZE the first table but do not run ZOrder

// COMMAND ----------

// MAGIC %sql
// MAGIC OPTIMIZE nonZorderButPartitioned

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM nonZorderButPartitioned WHERE frequency1=1

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Step 3 : ZOrder the table (default configs)

// COMMAND ----------

// MAGIC %sql
// MAGIC OPTIMIZE zorderDefaultButPartitioned ZORDER BY frequency1

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * from zorderDefaultButPartitioned WHERE frequency1=1

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 4: ZOrder the table but use 32MB Files

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TABLE zorder32MBButPartitioned SHALLOW CLONE delta.`${delta_table_path}`

// COMMAND ----------

val a = 32*1024*1024

// COMMAND ----------

// MAGIC %sql
// MAGIC ALTER TABLE zorder32MBButPartitioned SET TBLPROPERTIES (delta.targetFileSize=33554432)

// COMMAND ----------

// MAGIC %sql desc detail zorder32MBButPartitioned

// COMMAND ----------

// MAGIC %sql
// MAGIC OPTIMIZE zorder32MBButPartitioned ZORDER BY frequency1

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM zorder32MBButPartitioned WHERE frequency1=1

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 5: Remove partitioning from the table and run ZOrder with 32MB sizes again

// COMMAND ----------

spark.read.format("delta").table("zorder32MBButPartitioned").write.mode("overwrite").format("delta").saveAsTable("zorder32MB")

// COMMAND ----------

// MAGIC %sql 
// MAGIC desc detail zorder32MB

// COMMAND ----------

// MAGIC %sql
// MAGIC ALTER TABLE zorder32MB SET TBLPROPERTIES (delta.targetFileSize=33554432);
// MAGIC OPTIMIZE zorder32MB ZORDER BY frequency1;

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM zorder32MB WHERE frequency1=1

// COMMAND ----------


