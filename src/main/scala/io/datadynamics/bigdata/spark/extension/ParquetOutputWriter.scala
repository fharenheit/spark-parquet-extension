package io.datadynamics.bigdata.spark.extension

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter

/**
 * @author 김하늘
 */
class ParquetOutputWriter(path: String, context: TaskAttemptContext) extends OutputWriter {

  private val conf = context.getConfiguration
  private val partitionColumns = conf.getStrings(PrefixParquetFileFormat.PARTITION_COLUMNS_KEY)
  private val deletePartitionColumnName = conf.getBoolean(PrefixParquetFileFormat.DELETE_PARTITION_COLUMN_NAME_KEY, false)
  private val prefix = conf.get(PrefixParquetFileFormat.PREFIX_KEY, "prefix")

  private val recordWriter = new ParquetOutputFormat[InternalRow]() {
    override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
      val p = new Path(path)
      val parent = if (deletePartitionColumnName) this.getNewPath(p.getParent, partitionColumns) else p.getParent
      new Path(parent, s"${prefix}-${p.getName}")
      //new Path(path, f"part-$split%05d-$uniqueWriteJobId$bucketString$extension")
    }

    private def getNewPath(parent: Path, partitionColumns: Array[String]) = {
      val parentFragments = parent.toString.split("/")
      // partition column 이전까지 path
      val newParent = parentFragments.slice(0, parentFragments.length - partitionColumns.length)
      // partition column 지워진 path
      val replacedPath = parentFragments.reverse.zip(partitionColumns.reverse).map(pathColumn => {
        val (path: String, column: String) = pathColumn
        path.substring(column.length + 1)
      }).reverse
      val newPath = (newParent ++ replacedPath).mkString("/")
      new Path(newPath)
    }
  }.getRecordWriter(context)

  override def write(row: InternalRow): Unit = recordWriter.write(null, row)

  override def close(): Unit = recordWriter.close(context)
}