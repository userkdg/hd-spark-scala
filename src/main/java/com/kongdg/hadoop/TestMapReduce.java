package com.kongdg.hadoop;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * 针对hbase中的Family BC簇，B的列为kv格式，C为图片、二进制文件
 *
 * @author userkdg
 * @date 2019/12/21 10:24
 **/
public class TestMapReduce {
    public static void main(String[] args) {
        try {
            Configuration conf = HBaseConfiguration.create();
//            Configuration conf = new Configuration();
//        conf.set
            Job job = Job.getInstance(conf);
            job.setMapperClass(TestMapper.class);
            job.setJobName("export hbase data");
            job.setJarByClass(TestMapReduce.class);

            Scan scan = new Scan();

            //过滤我们想要的数据
//            scan.addFamily(Bytes.toBytes("ext"));
//            scan.addColumn(Bytes.toBytes("ext"), Bytes.toBytes("userId"));
//            scan.addColumn(Bytes.toBytes("ext"), Bytes.toBytes("regTime"));

            scan.setBatch(1000);
            scan.setCacheBlocks(false);

            TableMapReduceUtil.initTableMapperJob(
                    "USER_TABLE",
                    scan,
                    TestMapper.class,
                    Text.class,
                    Map.class,
                    job
            );
            final String dirStr = "/tmp/hbasecsv";
            Path dirPath = new Path(dirStr);
            FileSystem fs = FileSystem.get(conf);
            fs.deleteOnExit(dirPath);

            job.setReducerClass(TestReducer.class);
            JobConf jobConf = new JobConf(job.getConfiguration());
            jobConf.set(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR, dirPath.toString());
            FileOutputFormat.setOutputPath(jobConf, dirPath);
            boolean isDone = job.waitForCompletion(true);
            System.out.println(isDone);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    private class TestReducer extends Reducer<Text, Map<Text, Text>, Text, List<Text>> {
        private Gson gson = new Gson();

        @Override
        protected void reduce(Text key, Iterable<Map<Text, Text>> values, Context context) throws IOException, InterruptedException {
            List<Text> texts = Lists.newArrayList();
            for (Map<Text, Text> textTextMap : values) {
                String jsonRow = gson.toJson(textTextMap);
                texts.add(new Text(jsonRow.getBytes()));
            }
            context.write(key, texts);
        }
    }

    private class TestMapper extends TableMapper<Text, Map<Text, Text>> {
        private static final String FAMILY_NAME = "B";

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            NavigableMap<byte[], byte[]> familyMap = value.getFamilyMap(Bytes.toBytes(FAMILY_NAME));
            Map<Text, Text> BMap = Maps.newLinkedHashMap();
            familyMap.forEach((col, colVal) -> BMap.put(new Text(col), new Text(colVal)));
            if (!BMap.isEmpty()) {
                context.write(new Text(key.get()), BMap);
            }
        }
    }
}
