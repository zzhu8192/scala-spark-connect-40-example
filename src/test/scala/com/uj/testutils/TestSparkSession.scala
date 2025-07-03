package com.uj.testutils

import ch.qos.logback.classic.Level
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}
import org.scalatest._
import org.slf4j.LoggerFactory

import java.io.ByteArrayOutputStream

object TestSparkSession {
  def createSparkSession: SparkSession = {
    val connectUrl = sys.env.getOrElse("SPARK_CONNECT_URL", "sc://localhost:15002")
    SparkSession.builder()
      .remote(connectUrl)
      /*
            .config("spark.ui.enabled", value = false)
            .config("spark.ui.showConsoleProgress", value = false)
      //      .config("spark.driver.bindAddress", value = "127.0.0.1")
            .config("spark.driver.memory", "8g")
            .config("spark.executor.memory", "8g")
            .config("spark.driver.host", "localhost")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryo.unsafe", "true")
            .config("spark.kryoserializer.buffer", "64k")
            .config("spark.serializer.objectStreamReset", "-1")
            .config("spark.broadcast.compress", "true")
      //      .config("spark.rdd.compress", "true")
            .config("spark.shuffle.compress", "true")
            // spark sql tweaks
            .config("spark.sql.inMemoryColumnarStorage.compressed", "true")
            .config("spark.sql.optimizer.metadataOnly ", "false")
            .config("spark.sql.inMemoryColumnarStorage.compressed ", "true")
            .config("spark.sql.orc.compression.codec ", "true")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.optimizer.maxIterations", "5")
            .config("spark.sql.parquet.compression.codec", "zstd")
            .config("parquet.compression.codec.zstd.level", "1")
            // enable delta support
           // .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          //  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          //  .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            // delta specific optimizations for tests
            .config("spark.databricks.delta.autoCompact.enabled", "false")
            .config("spark.databricks.delta.optimizeWrite.enabled", "false")
            .config("spark.databricks.delta.logRetentionDuration", "0 days")
            .config("spark.databricks.delta.deletedFileRetention", "0 days")
            .config("spark.databricks.io.cache.enabled", "false")
        //    .config("spark.databricks.delta.snapshotPartitions", "1")
         //   .config("delta.log.cacheSize", "8")
            .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "4")
            .enableHiveSupport()

       */
      .getOrCreate()
  }

  lazy val parentSparkSession: SparkSession = createSparkSession
  private val sync = new Object
  var warnOnceDone = false
}

trait TestSparkSession extends BeforeAndAfterAll { this: Suite =>

  private var sparkSessionInternal: SparkSession = _

  def setLogLevels(level: Level, loggers: Seq[String]): Map[String, Level] = loggers.map(loggerName => {
    val logger = LoggerFactory.getLogger(loggerName).asInstanceOf[ch.qos.logback.classic.Logger]
    val prevLevel = logger.getLevel
    logger.setLevel(level)
    loggerName -> prevLevel
  }).toMap

  setLogLevels(Level.WARN, Seq("org.apache.spark", "org.apache.hadoop", "org.apache.parquet", "org.spark_project",
    "org.sparkproject", "org.eclipse.jetty", "akka", "io.netty", "com.jolbox.bonecp.BoneCPDataSource"))
  setLogLevels(Level.ERROR, Seq("org.apache.hadoop.hive.conf.HiveConf", "org.apache.hadoop.hive.metastore.ObjectStore", "hive.ql.metadata.Hive", "hive.log", "hive.metastore", "org.apache.hadoop.hive.ql.session.SessionState"))
  setLogLevels(Level.OFF, Seq("org.apache.hadoop.hive.metastore.RetryingHMSHandler"))

  if (!TestSparkSession.warnOnceDone) {
    //LoggerFactory.getLogger(this.getClass).warn("Spark logging has been disabled.")
    TestSparkSession.warnOnceDone = true
  }

  implicit protected def sparkSession: SparkSession = {
    if (sparkSessionInternal == null) {
      createSession()
    }
    sparkSessionInternal
  }

  def mkDataFrame[T : Encoder](entities: T*)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    entities.toDF()
  }



  abstract override def run(testName: Option[String], args: Args): Status = {
    TestSparkSession.sync.synchronized(
      super.run(testName, args.copy(distributor = None))
    )
  }

  abstract override def runNestedSuites(args: Args): Status = {
    TestSparkSession.sync.synchronized(
      super.runNestedSuites(args.copy(distributor = None))
    )
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally cleanupSession()
  }

  private def cleanupSession(): Unit = {

    if(sparkSessionInternal != null) {
      // sparkSessionInternal.close()
      // we can clear the cache to reduce test suite memory footprint
      clearCache()

      sparkSessionInternal = null
    }
  }

  private def clearCache(): Unit = {
    if(sparkSessionInternal != null) {
      /*
      sparkSessionInternal.sqlContext.clearCache()
      sparkSessionInternal.sparkContext.getPersistentRDDs
        .foreach { case (_, rdd) => rdd.unpersist(true) } */
    }
  }

  private def createSession(): Unit = {
    sparkSessionInternal = TestSparkSession.parentSparkSession.newSession()
  }
}
