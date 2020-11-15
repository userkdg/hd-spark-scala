//package com.kongdg.sqoop;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.sqoop.Sqoop;
//import org.apache.sqoop.tool.SqoopTool;
//import org.apache.sqoop.util.OptionsFileUtil;
//
//import java.sql.Timestamp;
//import java.util.Date;
//
///**
// * @author userkdg
// * @date 2020-05-30 18:49
// **/
//public class SqoopStudy {
//    public static void main(String[] args) throws Exception {
//        sqoopDb2Db();
//        db2Hbase();
//    }
//
//    private static void db2Hbase() throws Exception {
//        String[] args = new String[] {
//                "--connect",jdbc,
//                "--driver",driver,
//                "-username",username,
//                "-password",password,
//                "--table",mysqlTable,
//                "--hbase-table",hbaseTableName,
//                "--column-family",columnFamily,
//                "--hbase-create-table",
//                "--hbase-row-key",rowkey,
//                "-m",String.valueOf(m),
//        };
//        SqoopBean sqoopBean = new SqoopBean();
//        String[] expandArguments = OptionsFileUtil.expandArguments(args);
//        SqoopTool tool = SqoopTool.getTool("import");
//        Configuration conf = new Configuration();
//        Configuration loadPlugins = SqoopTool.loadPlugins(conf);
//        Sqoop sqoop = new Sqoop((com.cloudera.sqoop.tool.SqoopTool) tool, loadPlugins);
//        sqoopBean.setId(Sqoop.runSqoop(sqoop,expandArguments));
//        sqoopBean.setTs(new Timestamp(System.currentTimeMillis()));
//    }
//
//    private static void sqoopDb2Db() throws Exception {
//        String[] args = new String[] {
//                "--connect",jdbc,
//                "--driver",driver,
//                "-username",username,
//                "-password",password,
//                "--table",table,
//                "-m",String.valueOf(m),
//                "--target-dir",targetdir,
//        };
//
//        SqoopBean sqoopBean = new SqoopBean();
//        String[] expandArguments = OptionsFileUtil.expandArguments(args);
//        SqoopTool tool = SqoopTool.getTool("import");
//        Configuration conf = new Configuration();
//        conf.set("fs.default.name", "192.168.1.5:9000");//设置HDFS服务地址
//        Configuration loadPlugins = SqoopTool.loadPlugins(conf);
//        Sqoop sqoop = new Sqoop((com.cloudera.sqoop.tool.SqoopTool) tool, loadPlugins);
//        sqoopBean.setId(Sqoop.runSqoop(sqoop,expandArguments));
//        sqoopBean.setTs(new Timestamp(System.currentTimeMillis()));
//        // map.put("result",Sqoop.runSqoop(sqoop,expandArguments));  map.put("time",new Timestamp(new Date().getTime()));
//        ""
//    }
//
//}
