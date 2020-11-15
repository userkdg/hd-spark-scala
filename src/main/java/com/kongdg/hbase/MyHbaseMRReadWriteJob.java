package com.kongdg.hbase;

import com.kongdg.spark.base.BaseInit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author userkdg
 * @date 2020-05-27 23:47
 **/
public class  MyHbaseMRReadWriteJob extends BaseInit {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = HBaseConfiguration.create();
        config.addResource(new Path("F:\\j_workspace\\hd-spark-scala\\src\\main\\resources\\hbase", "hbase-site.xml"));
        config.addResource(new Path("F:\\j_workspace\\hd-spark-scala\\src\\main\\resources\\hbase", "core-site.xml"));
        Job job = new Job(config,"ExampleReadWrite");
        job.setJarByClass(MyHbaseMRReadWriteJob.class);    // class that contains mapper

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
// set other scan attrs

        byte[] sourceTable = Bytes.toBytes("test");
        TableMapReduceUtil.initTableMapperJob(
                sourceTable,      // input table
                scan,             // Scan instance to control CF and attribute selection
                MyMapper.class,   // mapper class
                null,             // mapper output key
                null,             // mapper output value
                job, false);
        String targetTable = "test_copy";
        TableMapReduceUtil.initTableReducerJob(
                targetTable,      // output table
                null,             // reducer class
                job, null, null,null, null, false);
        job.setNumReduceTasks(0);

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }
    public static class MyMapper extends TableMapper<ImmutableBytesWritable, Put> {

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            // this example is just copying the data from the source table...
            context.write(row, resultToPut(row,value));
        }

        private static Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
            Put put = new Put(key.get());
            for (Cell cell : result.listCells()) {
                put.add(cell);
            }
            return put;
        }
    }
}
