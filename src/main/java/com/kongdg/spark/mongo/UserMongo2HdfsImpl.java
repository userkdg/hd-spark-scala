package com.kongdg.spark.mongo;

import com.kongdg.spark.file.HdfsFileType;
import org.apache.spark.SparkConf;

/**
 * @author userkdg
 * @date 2019/12/8 0:58
 **/
public class UserMongo2HdfsImpl extends AbstractMongo2HdfsExecutor {
    public UserMongo2HdfsImpl(SparkConf sparkConf) {
        super(sparkConf);
    }

    public static void main(String[] args) {
        UserMongo2HdfsImpl mongo2Hdfs = new UserMongo2HdfsImpl(new SparkConf());
        mongo2Hdfs.setMongoInputUri("mongo:user/xx")
                .setMongoOutputUri("mongo:user/xx")
                .setToHdfsPath("/user/xxx/mongo/data/")
                .setHdfsFileType(HdfsFileType.PARQUET)
                .submit();
        boolean idDone = mongo2Hdfs.isDone();
        System.out.println(idDone);

    }

}
