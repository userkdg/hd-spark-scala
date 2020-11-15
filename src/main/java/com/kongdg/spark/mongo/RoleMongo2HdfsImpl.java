package com.kongdg.spark.mongo;

import com.kongdg.spark.file.HdfsFileType;
import org.apache.spark.SparkConf;

/**
 * @author userkdg
 * @date 2019/12/8 0:58
 **/
public class RoleMongo2HdfsImpl extends AbstractMongo2HdfsExecutor {
    public RoleMongo2HdfsImpl(SparkConf sparkConf) {
        super(sparkConf);
    }

    public static void main(String[] args) {
        RoleMongo2HdfsImpl mongo2Hdfs = new RoleMongo2HdfsImpl(new SparkConf());
        mongo2Hdfs.setMongoInputUri("mongo:urixxx")
                .setMongoOutputUri("")
                .setToHdfsPath("/user/xxx/mongo/data/")
                .setHdfsFileType(HdfsFileType.PARQUET)
                .submit();
    }

}
