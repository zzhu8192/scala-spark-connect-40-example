package com.uj.example

import org.apache.spark.sql.functions.{col, length, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Example {
  val CSVSparkReadOptions: Map[String, String] = Map[String, String](elems =
    "header" -> "true",
    "inferSchema" -> "false",
    "delimiter" -> ",",
    "quote" -> "\"",
    "encoding" -> "UTF-8",
    "ignoreLeadingWhiteSpace" -> "true",
    "ignoreTrailingWhiteSpace" -> "true"
  )

  /**
   * basic SQL based transform
   * @param spark
   * @param df
   * @return
   */
  def doTransformWithSQL(spark: SparkSession, df: DataFrame): DataFrame = {
    df.createOrReplaceTempView("df")
    spark.sql("select *,length(name) as len from df")
  }

  /**
   * Basic API based transform
   *
   * @param spark
   * @param df
   * @return
   */
  def doTransformWithAPI(spark: SparkSession, df: DataFrame): DataFrame = {
    df.withColumn("len", length(col("name")))
  }

  private def evenRange(start:Int, n:Int):Array[Int] = {
    Range(if (start % 2 == 0) start else start +1 , start+2*n, 2).toArray
  }

  private final def registerTestRangeUDF(spark:SparkSession):Unit = {
    val testUDF = udf((start:String, n:String) => evenRange(start.toInt, n.toInt))
    spark.udf.register("range_udf", testUDF)
  }

  /**
   * UDF on dataframe
   * @param spark
   * @param df
   * @return
   */
  def doRangeTransformWithUDF(spark: SparkSession, df: DataFrame): DataFrame = {
    registerTestRangeUDF(spark)
    df.createOrReplaceTempView("test_table")
    spark.sql("select *, range_udf(start, n) as range from test_table")
  }

  /**
   * basic map on dataframe
   * @param spark
   * @param df
   * @return
   */
  def doRangeTransform(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    df.map( x => ( x.getString(0).toInt, x.getString(1).toInt, evenRange(x.getString(0).toInt, x.getString(1).toInt))).toDF("start", "count", "range")
  }

  // TODO
  // 1. UDF example
  // 2. JSON example
  // 3. composition?
  // 4. streaming

  def doETL(spark: SparkSession, inputPath: String, outputPath: String): DataFrame = {
    println(s"-------------- doETL() reading csv $inputPath $outputPath")
    val df = spark.read.format("csv").options(CSVSparkReadOptions).load(inputPath)
    df.show()
    val resultDf=doTransformWithAPI(spark, df)
    resultDf.show()
    resultDf.write.mode("overwrite").format("parquet").option("compression", "gzip").save(outputPath)
    createHiveExternalTableOnParquet(spark, outputPath, "example_db", "example_table", null)
  }

  /**
   * Create Hive external Table from existing Parquet
   *
   * @param spark
   * @param parquetPath
   * @param db
   * @param table
   * @param partitionKey
   * @return
   */
  private def createHiveExternalTableOnParquet(spark: SparkSession,
                                               parquetPath: String,
                                               db: String,
                                               table: String,
                                               partitionKey: String=null): DataFrame = {

    println(s"createHiveExternalTableOnParquet() building external hive tables for DB: $db.$table from: $parquetPath")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $db")
    spark.sql(s"DROP TABLE IF EXISTS $db.$table")

    val df = spark.read.format("parquet").option("mergeSchema", "true").load(parquetPath)
    val ddlSchemaString=df.schema.fields.map(field => s"${field.name} ${field.dataType.sql}").mkString(",")

    val createExternalSql =
      s"CREATE TABLE IF NOT EXISTS $db.$table ($ddlSchemaString)"+
        s" USING PARQUET LOCATION '$parquetPath'" +
        (if (partitionKey !=null) s" PARTITIONED BY ($partitionKey) " else "")
    println(s"$db.$table sql: $createExternalSql")
    val result=spark.sql(createExternalSql)
    if (partitionKey != null)
      spark.sql(s"MSCK REPAIR TABLE $db.$table ")
    result
  }

  /**
   * Create Hive external Table from dataframe with explicit location
   * You can also use saveAsTable(), but that has potential contention related issues
   *
   * @param spark
   * @param df
   * @param hiveDir
   * @param db
   * @param table
   * @param sql
   * @return
   */
  def createHiveTableFromDf(spark: SparkSession,
                            df: DataFrame,
                            hiveDir: String,
                            db: String,
                            table: String,
                            sql: String = "",
                       ): DataFrame = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $db")
    spark.sql(s"DROP TABLE IF EXISTS $db.$table")
    val tempTableName="tmp"+System.currentTimeMillis()+"_"+table
    df.createOrReplaceTempView(tempTableName)
    val sqlString=if (sql.isEmpty) s"SELECT * FROM $tempTableName" else sql
    val createTableSql=s"CREATE TABLE IF NOT EXISTS $db.$table USING PARQUET LOCATION '$hiveDir' AS $sqlString"
    println(s"createHiveTableFromDf() build materialized external hive tables for DB: $createTableSql")
    spark.sql(createTableSql)
  }

  def main(args: Array[String]): Unit = {
    val connectUrl = sys.env.getOrElse("SPARK_CONNECT_URL", sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15002"))
    val baseSparkSession = SparkSession.builder()
      .remote(connectUrl)
      .getOrCreate()

    val inputPath = if (args.length > 0) args(0) else "src/main/resources/test.csv"
    val outputPath = if (args.length > 1) args(1) else "/tmp/spark-connect-example-output"
    doETL(baseSparkSession, inputPath, outputPath).show()
  }
}

/*
[
  {
      "Name": "Test",
      "Type": "Spark",
      "Args": [
        "--class", "com.uj.example.Example",
        "--deploy-mode", "cluster",
        "--master","yarn",

        "--conf", "spark.io.compression.codec=snappy",
        "--conf", "spark.kryo.unsafe=true",
        "--conf", "spark.sql.parquet.writeLegacyFormat=true",
        "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
        "--conf", "spark.yarn.maxAppAttempts=1",
        "--conf", "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2",

        "--conf", "spark.dynamicAllocation.enabled=true",
        "--conf", "spark.dynamicAllocation.minExecutors=8",
        "--conf", "spark.dynamicAllocation.executorIdleTimeout=120s",

        "--conf", "spark.driver.memory=11g",
        "--conf", "spark.executor.memory=40g",
        "--conf", "spark.executor.cores=4",
        "--conf", "spark.driver.cores=4",
        "--conf", "spark.driver.maxResultSize=11g",
        "--conf", "spark.executor.memoryOverhead=4g",
      ]
  }
]
 */