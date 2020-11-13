package io.datadynamics.bigdata.spark.extension

import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.parquet.hadoop.codec.CodecConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

/**
 * @author 김하늘
 */
class PrefixParquetFileFormat extends ParquetFileFormat {

  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = {
    super.prepareWrite(sparkSession, job, options, dataSchema)

    new OutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String = {
        CodecConfig.from(context).getCodec.getExtension + ".parquet"
      }

      override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
        new ParquetOutputWriter(path, context)
      }
    }
  }
}

object PrefixParquetFileFormat {
  val REDIS_ENABLED = "redis.enabled"
  val REDIS_SERVER = "redis.server"
  val REDIS_CHANNEL_NAME = "redis.channel.name"
  val PARTITION_COLUMNS_KEY = "parquet.partition.columns"
  val DELETE_PARTITION_COLUMN_NAME_KEY = "parquet.delete.partition.column.name"
  val PREFIX_KEY = "parquet.prefix"
}