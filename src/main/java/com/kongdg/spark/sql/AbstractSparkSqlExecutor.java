package com.kongdg.spark.sql;

import com.kongdg.spark.base.AbstractSparkJobExecutor;
import com.kongdg.spark.file.HdfsFileType;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * @author userkdg
 * @date 2019/12/8 0:20
 **/
public abstract class AbstractSparkSqlExecutor extends AbstractSparkJobExecutor implements Serializable {
    public static final HdfsFileType DEFAULT_FILE_TYPE = HdfsFileType.ORC;
    private static final long serialVersionUID = 1L;
    private HdfsFileType hdfsFileType = DEFAULT_FILE_TYPE;
    /**
     * source
     *
     * @return
     */
    private String dataFromHdfsPath;

    private String sql;

    /*
    target
     */
    private String outputPathOrTable;


    protected AbstractSparkSqlExecutor(SparkConf sparkConf) {
        super(sparkConf);
    }


    @Override
    protected String sparkAppName() {
        return "spark sql calc data";
    }

    @Override
    protected void checkRunJobPreParams() {
        if (sql == null) {

        }
        if (dataFromHdfsPath == null) {

        }
    }

    @Override
    protected boolean runJob(SparkSession sparkSession, JavaSparkContext javaSparkContext) {
        LOG.info("use spark sql engine {},{},{}", dataFromHdfsPath, sql, hdfsFileType);
        sparkSession.sql(sql).write().parquet(outputPathOrTable);
        return true;
    }

    @Override
    protected void checkRunJobAfterParams() {

    }

    public HdfsFileType getHdfsFileType() {
        return hdfsFileType;
    }

    public AbstractSparkSqlExecutor setHdfsFileType(HdfsFileType hdfsFileType) {
        this.hdfsFileType = hdfsFileType;
        return this;
    }

    public String getDataFromHdfsPath() {
        return dataFromHdfsPath;
    }

    public AbstractSparkSqlExecutor setDataFromHdfsPath(String dataFromHdfsPath) {
        this.dataFromHdfsPath = dataFromHdfsPath;
        return this;
    }

    public String getSql() {
        return sql;
    }

    public AbstractSparkSqlExecutor setSql(String sql) {
        this.sql = sql;
        return this;
    }

    public String getOutputPathOrTable() {
        return outputPathOrTable;
    }

    public AbstractSparkSqlExecutor setOutputPathOrTable(String outputPathOrTable) {
        this.outputPathOrTable = outputPathOrTable;
        return this;
    }
}
