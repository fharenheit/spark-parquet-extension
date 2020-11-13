package io.datadynamics.bigdata.spark.extension

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Assert, Test}
import org.slf4j.{Logger, LoggerFactory}

/**
 * (현재 상황) Spark 에서 spark.partitionBy().parquet()를 하여 파티션을 생성하여 파케이 파일로 저장하고 있습니다.
 *
 * @author 김하늘
 */
class Exam {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("exam")
  lazy val spark: SparkSession = SparkSession.builder().config(sparkConf) /*.enableHiveSupport()*/ .getOrCreate()
  lazy val sc: SparkContext = spark.sparkContext

  val rows = Seq(
    Row(0, "2020", "11", "10")
    , Row(1, "2020", "11", "11")
    , Row(2, "2020", "11", "12")
    , Row(3, "2020", "11", "13")
    , Row(4, "2020", "11", "14")
    , Row(5, "2020", "11", "15")
    , Row(6, "2021", "11", "16")
    , Row(7, "2021", "11", "17")
    , Row(8, "2021", "11", "18")
    , Row(9, "2021", "11", "19")
  )

  /**
   * (문제 1) 파티션 생성시 파티션 경로 (예; yyyy=2020) 식을 2020만 들어가도록 했으면 좋겠습니다.
   * (문제 2) 디렉토리에 Parquet 파일 생성시 파일 Prefix 지정할 수 있는 방법이 있으면 좋겠습니다.
   */
  @Test
  def solve(): Unit = {
    // REDIS CONFIGURATION
    sc.hadoopConfiguration.setBoolean(PrefixParquetFileFormat.REDIS_ENABLED, true)
    sc.hadoopConfiguration.set(PrefixParquetFileFormat.REDIS_CHANNEL_NAME, "test")
    sc.hadoopConfiguration.set(PrefixParquetFileFormat.REDIS_SERVER, "localhost")

    // PARTITION CONFIGURATION
    sc.hadoopConfiguration.set(PrefixParquetFileFormat.PARTITION_COLUMNS_KEY, "yyyy,MM")
    sc.hadoopConfiguration.setBoolean(PrefixParquetFileFormat.DELETE_PARTITION_COLUMN_NAME_KEY, true)

    // FILE PREFIX CONFIGURATION
    sc.hadoopConfiguration.set(PrefixParquetFileFormat.PREFIX_KEY, "myPrefix")

    val rowRdd: RDD[Row] = sc.parallelize(rows)
    val schema: StructType = StructType(Array(
      StructField("i", IntegerType)
      , StructField("yyyy", StringType)
      , StructField("MM", StringType)
      , StructField("dd", StringType)
    ))
    val df: DataFrame = spark.createDataFrame(rowRdd, schema)

    val outputPath = "target/output"
    val writer: DataFrameWriter[Row] = df.write
      .format("io.datadynamics.bigdata.spark.extension.PrefixParquetFileFormat")
      .option("basePath", "basePath")
      .option("basePath", "basePath")
    val value: DataFrameWriter[Row] = writer.partitionBy("yyyy", "MM")
    value.save(outputPath)
  }

  @Test
  def newPathTest(): Unit = {
    val partitionColumns: Array[String] = Array("yyyy", "MM")
    val path = "file:/attempt_20201110225352_0000_m_000000_0/yyyy=2020/MM=11/d46.c000.snappy.parquet"
    val p = new Path(path)
    val parent: Path = p.getParent

    val parentFragments: Array[String] = parent.toString.split("/")
    logger.info(s"parentFragments = ${parentFragments.mkString("Array(", ", ", ")")}")
    // partition column 이전까지 path
    val newParent: Array[String] = parentFragments.slice(0, parentFragments.length - partitionColumns.length)
    logger.info(s"newParent = ${newParent.mkString("Array(", ", ", ")")}")
    // partition column 지워진 path
    val replacedPath: Array[String] = parentFragments.reverse.zip(partitionColumns.reverse).map(pathColumn => {
      val (path: String, column: String) = pathColumn
      path.substring(column.length + 1)
    }).reverse
    val newPath: String = (newParent ++ replacedPath).mkString("/")
    Assert.assertEquals("file:/attempt_20201110225352_0000_m_000000_0/2020/11", newPath)
  }
}