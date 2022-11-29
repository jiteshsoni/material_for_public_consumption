# Databricks notebook source
# MAGIC %md
# MAGIC ## Reference
# MAGIC [How to read the Spark UI](https://www.youtube.com/watch?v=_Ne27JcLnEc&t=1177s)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is the demo about ?
# MAGIC 
# MAGIC #### Show performance difference between
# MAGIC 1.) compacting small files (optimize)    
# MAGIC 2.) Compatcing the files and  sorting/colocating the data in files ( Optimize and Z-order )    
# MAGIC 3.) Partition the data and Compatcing the files and  sorting/colocating the data in files ( Partitioning and Optimize and Z-order )

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Layout Demo

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC def clear_all_cache():
# MAGIC     spark.conf.set("spark.databricks.io.cache.enabled", False)
# MAGIC     spark.conf.set("spark.databricks.delta.smallTable.cache.enabled", "false")
# MAGIC     spark.conf.set("spark.databricks.delta.stats.localCache.maxNumFiles", "1")
# MAGIC     spark.conf.set("spark.databricks.delta.fastQueryPath.dataskipping.checkpointCache.enabled", "false")
# MAGIC     spark.sql("CLEAR CACHE")
# MAGIC     sqlContext.clearCache()
# MAGIC 
# MAGIC clear_all_cache()
# MAGIC 
# MAGIC def clear_cache_and_run_sql(sql_query: str):
# MAGIC     clear_all_cache()
# MAGIC     display(
# MAGIC         spark.sql(sql_query)
# MAGIC     )    

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT delta_table_name DEFAULT "data_layout_demo"
# MAGIC --CREATE WIDGET TEXT delta_table_path DEFAULT "/tmp/optimize_zorder_demo/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Delta Table 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS `${delta_table_name}` (
# MAGIC   identity BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   partition_number int,
# MAGIC   current_date DATE,
# MAGIC   frequency1 LONG,
# MAGIC   frequency10 LONG,
# MAGIC   frequency100 LONG,
# MAGIC   frequency1000 LONG,
# MAGIC   frequency10000 LONG,
# MAGIC   frequency100000 LONG
# MAGIC )
# MAGIC PARTITIONED BY(partition_number);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate data

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC for (fileNum <- 1 to 1000) {
# MAGIC   for (part <- 1 to 100) {
# MAGIC     val total = 10000000
# MAGIC     spark.range(total)
# MAGIC       .withColumn("partition_number", lit(part))
# MAGIC       .withColumn("current_date", current_date())
# MAGIC       .withColumn("frequency1", $"id" % total)
# MAGIC       .withColumn("frequency10", $"id" % (total / 10))
# MAGIC       .withColumn("frequency100", $"id" % (total / 100))
# MAGIC       .withColumn("frequency1000", $"id" % (total / 1000))
# MAGIC       .withColumn("frequency10000", $"id" % (total/ 10000))
# MAGIC       .withColumn("frequency100000", $"id" % (total / 100000))
# MAGIC       .repartition(1)
# MAGIC       .write
# MAGIC       .format("delta")
# MAGIC       .mode("append")
# MAGIC       .option("mergeSchema", "true")
# MAGIC       .saveAsTable("data_layout_demo")
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC DETAIL `${delta_table_name}`

# COMMAND ----------

# MAGIC %md
# MAGIC 2123 files with 300 GB which is roughly 140 MB per files

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED `${delta_table_name}`

# COMMAND ----------

# MAGIC %sql
# MAGIC --13,874,500,000
# MAGIC Select count(*) 
# MAGIC from  `${delta_table_name}`

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from  `${delta_table_name}`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   partition_number,
# MAGIC   count(1) as row_count
# MAGIC FROM `${delta_table_name}` 
# MAGIC GROUP BY 
# MAGIC   partition_number
# MAGIC ORDER BY 
# MAGIC   partition_number

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create clones of the table (just so that experiments are easier)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE non_zorder_but_partitioned SHALLOW CLONE `${delta_table_name}`

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTIMIZE the first table but do not run ZOrder. 
# MAGIC 
# MAGIC This will only solve the small file problem

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE non_zorder_but_partitioned

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC DETAIL non_zorder_but_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC The below one just relying on Delta Log statistics to hit the right file

# COMMAND ----------

clear_all_cache()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM non_zorder_but_partitioned 
# MAGIC WHERE identity= 9000

# COMMAND ----------

# MAGIC %md
# MAGIC The below one just relying on Delta Log statistics and Partition prune

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM non_zorder_but_partitioned 
# MAGIC WHERE identity=1000
# MAGIC   and partition_number = 1

# COMMAND ----------

# MAGIC %md 
# MAGIC ### ZOrder the table (default configs)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE zorder_default_but_partitioned SHALLOW CLONE `${delta_table_name}`

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE zorder_default_but_partitioned ZORDER BY identity

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC from zorder_default_but_partitioned 
# MAGIC WHERE identity=1000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC from zorder_default_but_partitioned 
# MAGIC WHERE identity=1000
# MAGIC   and partition_number = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove partitioning from the table and run Zorder on identity columns

# COMMAND ----------

spark.read.format("delta").table('zorder_default_but_partitioned').write.mode("overwrite").format("delta").saveAsTable("zorder_on_identity_column")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE zorder_on_identity_column ZORDER BY (identity)

# COMMAND ----------

spark.read.format("delta").table("zorder_default_but_partitioned").write.mode("overwrite").format("delta").saveAsTable("zorder_on_id_column")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE zorder_on_id_column ZORDER BY (id)

# COMMAND ----------

spark.read.format("delta").table("zorder_on_id_column").write.mode("overwrite").format("delta").saveAsTable("zorder_on_identity_and_id_column")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE zorder_on_identity_and_id_column ZORDER BY (identity,id)

# COMMAND ----------

# MAGIC %sql
# MAGIC DEsc detail zorder_on_identity_column

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM zorder_on_identity_column 
# MAGIC WHERE identity =99

# COMMAND ----------

# MAGIC %sql
# MAGIC DEsc detail zorder_on_identity_and_id_column

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM zorder_on_identity_and_id_column 
# MAGIC WHERE identity =99

# COMMAND ----------


