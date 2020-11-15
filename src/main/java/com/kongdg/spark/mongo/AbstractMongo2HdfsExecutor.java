package com.kongdg.spark.mongo;

import com.kongdg.spark.base.AbstractSparkJobExecutor;
import com.kongdg.spark.file.HdfsFileType;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import java.io.Serializable;
import java.util.Collections;

/**
 * @author userkdg
 * @date 2019/12/8 0:20
 **/
public abstract class AbstractMongo2HdfsExecutor extends AbstractSparkJobExecutor implements Serializable {
    public static final HdfsFileType DEFAULT_FILE_TYPE = HdfsFileType.ORC;
    private static final long serialVersionUID = 1L;
    private HdfsFileType hdfsFileType = DEFAULT_FILE_TYPE;
    /**
     * source
     * val uri = """mongodb://xxx.xxx.xxx.xxx:27017/db.test,xxx.xxx.xxx.xxx:27017,xxx.xxx.xxx.xxx:27017"""
     *
     * @return
     */
    private String mongoInputUri;
    private String mongoOutputUri;

    /**
     * target
     *
     * @return
     */
    private String toHdfsPath;

    protected AbstractMongo2HdfsExecutor(SparkConf sparkConf) {
        super(sparkConf);
    }

    @Override
    protected void checkRunJobPreParams() {
        if (mongoInputUri == null) {
        }
        sparkConf.set("spark.mongodb.input.uri", mongoInputUri);
        sparkConf.set("spark.mongodb.output.uri", mongoOutputUri);
    }

    @Override
    protected void checkRunJobAfterParams() {

    }

    @Override
    protected String sparkAppName() {
        return "extract mongo data 2 hdfs";
    }

    @Override
    protected boolean runJob(SparkSession sparkSession, JavaSparkContext javaSparkContext) {
        JavaMongoRDD<Document> javaMongoRDD = MongoSpark.load(javaSparkContext).withPipeline(Collections.singletonList(Document.parse("{ $match: { name :  'KDG'  } }")));
        Dataset<Row> rowDataset = javaMongoRDD.toDF();
        rowDataset.write().mode(SaveMode.Append).format(hdfsFileType.name()).save(toHdfsPath);
        LOG.info("use spark engine , mongo {} to hdfs {} and file type is {}", mongoInputUri, toHdfsPath, hdfsFileType);
        return true;
    }

    public HdfsFileType getHdfsFileType() {
        return hdfsFileType;
    }

    public AbstractSparkJobExecutor setHdfsFileType(HdfsFileType hdfsFileType) {
        this.hdfsFileType = hdfsFileType;
        return this;
    }

    public String getToHdfsPath() {
        return toHdfsPath;
    }

    public AbstractMongo2HdfsExecutor setToHdfsPath(String toHdfsPath) {
        this.toHdfsPath = toHdfsPath;
        return this;
    }

    public String getMongoOutputUri() {
        return mongoOutputUri;
    }

    public AbstractMongo2HdfsExecutor setMongoOutputUri(String mongoOutputUri) {
        this.mongoOutputUri = mongoOutputUri;
        return this;
    }

    public String getMongoInputUri() {
        return mongoInputUri;
    }

    public AbstractMongo2HdfsExecutor setMongoInputUri(String mongoInputUri) {
        this.mongoInputUri = mongoInputUri;
        return this;
    }
}
