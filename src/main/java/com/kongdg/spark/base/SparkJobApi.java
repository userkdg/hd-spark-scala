package com.kongdg.spark.base;

import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author userkdg
 * @date 2019/12/7 23:52
 **/
public interface SparkJobApi {
    Logger LOG = LoggerFactory.getLogger(SparkJobApi.class);

    void init(SparkConf conf);

    void submit();

    boolean isDone();
}
