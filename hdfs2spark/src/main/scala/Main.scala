//package com.dodat.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Main {
  def main(/*args: Array[String]*/): Unit = {
// spark-shell already has spark
//    val spark = SparkSession.builder()
//      .appName("spark-job")
//      .master("local[*]")
//      .config("spark.driver.bindAddress", "127.0.0.1")
//      .getOrCreate

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    // set spark.sql.legacy.timeParserPolicy to LEGACY to restore the behavior before Spark 3.0

    val activity_df = spark.read.parquet("hdfs://namenode:9000/raw_zone/fact/activity")
      .groupBy("timestamp", "student_code", "activity")
      .agg(sum("numberOfFiles").alias("totalFile"))
      .withColumnRenamed("timestamp", "date")
      .withColumn("date", date_format(to_date(col("date"), "MM/dd/yyyy"), "yyyyMMdd"))

    val student_df = spark.read.csv("hdfs://namenode:9000/danh_sach_sv_de.csv")
      .withColumn("_c0", col("_c0").cast(IntegerType))
      .withColumnRenamed("_c0", "student_code")
      .withColumnRenamed("_c1", "student_name")

    val result_df = activity_df.join(student_df, Seq("student_code"))
      .select("date", "student_code", "student_name", "activity", "totalFile")
      .orderBy("student_code", "date")
//    result_df.show()

//    spark.sql("set spark.sql.legacy.timeParserPolicy=CORRECTED")

    result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("hdfs://namenode:9000/output")

    val check_df = spark.read.option("header", "true").csv("hdfs://namenode:9000/output")
    check_df.printSchema()
    check_df.show()
  }
}