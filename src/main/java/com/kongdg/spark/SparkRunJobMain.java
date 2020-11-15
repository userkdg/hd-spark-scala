package com.kongdg.spark;

import com.kongdg.spark.base.SparkJobApi;
import com.kongdg.spark.mongo.UserMongo2HdfsImpl;
import org.apache.spark.SparkConf;

import java.io.IOException;
import java.util.Properties;

/**
 * @author userkdg
 * @date 2019/12/8 0:03
 **/
public class SparkRunJobMain {
    public static void main(String[] args) {
        // file:///data/SparkConfig.properties
        String configProp = System.getProperty("SparkConfig.conf");
        String resourceId = System.getProperty("resourceId");
        try {
            Properties properties = new Properties();
            properties.load(SparkRunJobMain.class.getResourceAsStream(configProp));
            SparkConf sparkConf = new SparkConf();
            properties.forEach((key, value) -> sparkConf.set((String) key, (String) value));
            SparkJobApi sparkJobApi = null;
            switch (resourceId) {
                case "1":
                    sparkJobApi = new UserMongo2HdfsImpl(sparkConf);
                    break;
                case "2":
                    break;
                default:
                    throw new UnsupportedOperationException("");
            }
            assert sparkJobApi != null;
            sparkJobApi.submit();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
