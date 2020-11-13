package io.datadynamics.bigdata.spark.extension

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter

class ParquetOutputWriter(path: String, context: TaskAttemptContext) extends OutputWriter {

  private val conf = context.getConfiguration
  private val basePath = conf.get("mapreduce.output.fileoutputformat.outputdir")
  private val redisEnabled = conf.getBoolean(PrefixParquetFileFormat.REDIS_ENABLED, false)
  private val redisChannelName = conf.get(PrefixParquetFileFormat.REDIS_CHANNEL_NAME)
  private val redisServer = conf.get(PrefixParquetFileFormat.REDIS_SERVER)
  private val partitionColumns = conf.getStrings(PrefixParquetFileFormat.PARTITION_COLUMNS_KEY)
  private val deletePartitionColumnName = conf.getBoolean(PrefixParquetFileFormat.DELETE_PARTITION_COLUMN_NAME_KEY, true)
  private val prefix = conf.get(PrefixParquetFileFormat.PREFIX_KEY, "prefix")
  private var realPath = ""

  private val recordWriter = new ParquetOutputFormat[InternalRow]() {
    override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
      // 태스크가 저장할 파일의 절대 경로(파티션 포함)
      val originalOutputFilePath = new Path(path)
      // 파티션을 포함한 상대 경로
      val partitionIncludedRelativeOutputFilePath = getPartitionIncludedRelativeOutputFilePath(originalOutputFilePath, partitionColumns)
      // 파티션 컬럼을 인식하여 처리한 경로
      val resolvedOutputPath = getResolvedOutputPath(originalOutputFilePath, partitionColumns)
      val filename = s"${prefix}-${originalOutputFilePath.getName}"
      val outputPath = new Path(resolvedOutputPath, filename)
      if (deletePartitionColumnName) {
        realPath = basePath + "/" + partitionIncludedRelativeOutputFilePath + "/" + filename
      } else {
        realPath = basePath + "/" + partitionIncludedRelativeOutputFilePath
      }
      if (redisEnabled) {
        println(s"메시지 전송 : ${realPath}")
        RedisUtils.send(redisServer, redisChannelName, outputPath.toString)
      }
      outputPath
    }

    /**
     * 임시 디렉토리, 파티션, 저장할 파일명을 모두 포함한 경로에서 파티션 및 저장할 파일명만 추출한 상대 경로를 반환한다.
     *
     * @param originalOutputFilePath 변형되지 않은 임시 디렉토리를 포함하는 파일 생성 경로
     * @param partitionColumns       파티션 컬럼 목록
     * @return 파티션 및 파일을 포함한 상대 경로
     */
    private def getPartitionIncludedRelativeOutputFilePath(originalOutputFilePath: Path, partitionColumns: Array[String]) = {
      if (deletePartitionColumnName) {
        val parentFragments = originalOutputFilePath.getParent.toString.split("/")
        val newParent = parentFragments.slice(0, parentFragments.length - partitionColumns.length)
        val replacedPath = parentFragments.reverse.zip(partitionColumns.reverse).map(pathColumn => {
          val (path: String, column: String) = pathColumn
          path.substring(column.length + 1)
        }).reverse
        val realPath = new Path((newParent ++ replacedPath).mkString("/"))
        val startIndex = PathUtils.getStartIndexOfPartitionIncludedRelativeOutputFilePath(path, partitionColumns.apply(0));
        realPath.toString.substring(startIndex)
      } else {
        // 파티션을 기준으로 분리한다.
        PathUtils.getPartitionIncludedRelativeOutputFilePath(path, partitionColumns.apply(0))
      }
    }

    private def getResolvedOutputPath(originalOutputFilePath: Path, partitionColumns: Array[String]) = {
      if (deletePartitionColumnName) {
        val parentFragments = originalOutputFilePath.getParent.toString.split("/")
        val newParent = parentFragments.slice(0, parentFragments.length - partitionColumns.length)
        val replacedPath = parentFragments.reverse.zip(partitionColumns.reverse).map(pathColumn => {
          val (path: String, column: String) = pathColumn
          path.substring(column.length + 1)
        }).reverse
        new Path((newParent ++ replacedPath).mkString("/"))
      } else {
        originalOutputFilePath.getParent
      }
    }
  }.getRecordWriter(context)

  override def write(row: InternalRow): Unit = recordWriter.write(null, row)

  override def close(): Unit = {
    recordWriter.close(context)
  }
}