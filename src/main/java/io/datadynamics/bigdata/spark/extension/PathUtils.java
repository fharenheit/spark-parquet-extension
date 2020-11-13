package io.datadynamics.bigdata.spark.extension;

import org.apache.commons.lang3.StringUtils;

public class PathUtils {

    public static String getPartitionIncludedRelativeOutputFilePath(String basePath, String partitionColumnName) {
        int index = getStartIndexOfPartitionIncludedRelativeOutputFilePath(basePath, partitionColumnName);
        return basePath.substring(index);
    }

    public static int getStartIndexOfPartitionIncludedRelativeOutputFilePath(String basePath, String partitionColumnName) {
        return StringUtils.indexOf(basePath, partitionColumnName);
    }

}
