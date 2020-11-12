package io.datadynamics.bigdata.spark.extension

import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.parquet.hadoop.codec.CodecConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

class PrefixParquetFileFormat extends ParquetFileFormat {

  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = {
    super.prepareWrite(sparkSession, job, options, dataSchema)

    new OutputWriterFactory {
      // This OutputWriterFactory instance is deserialized when writing Parquet files on the
      // executor side without constructing or deserializing ParquetFileFormat. Therefore, we hold
      // another reference to ParquetLogRedirector.INSTANCE here to ensure the latter class is initialized.

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
  val PARTITION_COLUMNS_KEY = "parquet.partition.columns"
  val DELETE_PARTITION_COLUMN_NAME_KEY = "parquet.delete.partition.column.name"
  val PREFIX_KEY = "parquet.prefix"
}