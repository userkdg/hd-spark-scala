package com.kongdg.spark.base;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * @author userkdg
 * @date 2019/12/8 0:01
 **/
public abstract class AbstractSparkJobExecutor implements SparkJobApi {
    /**
     * loading properties from file config
     * @param conf
     */
    static {
        // loading handler/commands when spark job done
    }

    protected volatile SparkConf sparkConf = new SparkConf().setAppName(sparkAppName());

    public AbstractSparkJobExecutor(SparkConf sparkConf) {
        init(sparkConf);
    }

    protected abstract String sparkAppName();

    protected abstract void checkRunJobPreParams();

    protected abstract boolean runJob(SparkSession sparkSession, JavaSparkContext javaSparkContext);

    protected abstract void checkRunJobAfterParams();

    @Override
    public void init(SparkConf conf) {
        if (conf != null) {
            conf.setAppName(sparkAppName());
            this.sparkConf = conf;
        }
    }
    private volatile boolean idDone;

    @Override
    public void submit() {
        SparkSession sparkSession = SparkUtils.getSpark(sparkConf);
        JavaSparkContext javaSparkContext = SparkUtils.getJsc(sparkConf);
        // 1 do
        checkRunJobPreParams();
        boolean done = runJob(sparkSession, javaSparkContext);
        checkRunJobAfterParams();
        //2 done ,to do other eg: calc
        this.idDone = done;
        if (done) {
            LOG.info("spark task {} is done", sparkAppName());
        }
    }

    @Override
    public boolean isDone() {
        return idDone;
    }
}
