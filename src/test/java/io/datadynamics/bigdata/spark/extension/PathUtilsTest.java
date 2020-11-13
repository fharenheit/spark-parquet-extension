package io.datadynamics.bigdata.spark.extension;

import org.junit.Assert;
import org.junit.Test;

public class PathUtilsTest {

    @Test
    public void getPartitionIncludedRelativeOutputFilePath() {
        String partitionIncludedRelativeOutputFilePath = PathUtils.getPartitionIncludedRelativeOutputFilePath("file:/Users/byounggonkim/projects/samsung/spark-parquet-extension/target/output/_temporary/0/_temporary/attempt_20201113165355_0000_m_000001_1/yyyy=2020/MM=11/part-00001-e225084c-0e2a-418f-8401-600bcb78a876.c000.snappy.parquet", "yyyy");
        Assert.assertEquals("yyyy=2020/MM=11/part-00001-e225084c-0e2a-418f-8401-600bcb78a876.c000.snappy.parquet", partitionIncludedRelativeOutputFilePath);
    }

    @Test
    public void getStartIndexOfPartitionIncludedRelativeOutputFilePath() {
        int startIndex = PathUtils.getStartIndexOfPartitionIncludedRelativeOutputFilePath("file:/Users/byounggonkim/projects/samsung/spark-parquet-extension/target/output/_temporary/0/_temporary/attempt_20201113165355_0000_m_000001_1/yyyy=2020/MM=11/part-00001-e225084c-0e2a-418f-8401-600bcb78a876.c000.snappy.parquet", "yyyy");
        Assert.assertEquals(143, startIndex);
    }

}