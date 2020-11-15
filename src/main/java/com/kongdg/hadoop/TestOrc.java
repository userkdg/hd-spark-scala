package com.kongdg.hadoop;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.orc.OrcFile;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author userkdg
 * @date 2020-04-11 22:41
 **/
public class TestOrc {
    public static void main(String[] args) throws IOException, SerDeException {
//        为了满足hadoop在windows上有权限操作 winutils.exe 否则会导致写入文件等报权限不足
        System.setProperty("hadoop.home.dir", "D:\\bigdata\\hadoop-2.7.2");
        Path path = new Path("F:\\j_workspace\\hd-spark-scala\\src\\main\\resources\\orc\\" +
                "part-00000-27422640-4f7d-41ff-a177-74f5ecc49d5a-c000.snappy.orc");
        Configuration conf = new Configuration();
        org.apache.orc.Reader reader = OrcFile.createReader(path,
                OrcFile.readerOptions(conf));
        RecordReader rows = reader.rows();
//        long rowNumber = rows.getRowNumber();
        long rowNumber = reader.getNumberOfRows();
        System.out.println("文件行数：" + rowNumber);
        List<String> fieldNames = reader.getSchema().getFieldNames();
        List<TypeDescription> types = reader.getSchema().getChildren();
        System.out.println("文件字段有：");
        for (int i = 0; i < fieldNames.size(); i++) {
            System.out.println(fieldNames.get(i) + "->" + types.get(i));
        }
        VectorizedRowBatch rowBatch = reader.getSchema().createRowBatch();
        Map<String,Object> rowsResult = Maps.newLinkedHashMap();
        while (rows.nextBatch(rowBatch)) {
            ColumnVector[] cols = rowBatch.cols;
            for (int j = 0; j < cols.length; j++) {
                ColumnVector col = cols[j];
                if (col instanceof LongColumnVector) {
                    LongColumnVector longColumnVector = (LongColumnVector) col;
                    long[] vector = longColumnVector.vector;
                    for (int i = 0; i < vector.length && i < rowNumber; i++) {
                        long value = vector[i];
                        System.out.println(value);
                    }
//                    StringBuilder buffer = new StringBuilder();
//                    for (long i = 0; i < rowNumber; i++) {
//                        longColumnVector.stringifyValue(buffer, (int) i);
//                        buffer.append("\n");
//                    }
//                    System.out.println(buffer.toString());
                }
            else if (col instanceof BytesColumnVector){
                    BytesColumnVector bytesColumnVector =(BytesColumnVector)col;
                    byte[][] vector = bytesColumnVector.vector;
                    for (int i = 0; i < vector.length && i < rowNumber; i++) {
                        byte[] value = vector[i];
                        System.out.println(Bytes.toString(value));
                    }
                }


                StringBuilder buffer = new StringBuilder();
                for (ColumnVector columnVector : cols) {
                    for (long i = 0; i < rowNumber; i++) {
                        if (columnVector instanceof BytesColumnVector){
                            ((BytesColumnVector)columnVector).stringifyValue(buffer, (int) i);
                        }
                        buffer.append((char)7);
                    }
//                    System.out.println(buffer.toString());
                }
                rowsResult.put(fieldNames.get(j), buffer.toString());
            }
        }
        System.out.println(rowsResult);
        for (Map.Entry<String, Object> entry : rowsResult.entrySet()) {
            String key = entry.getKey();
            String[] split = entry.getValue().toString().split(String.valueOf((char) 7));
            for (String s : split) {
                if ("name".equalsIgnoreCase(key)) {
//                    System.out.println(Bytes.toString(s));
                }
            }

        }
        rows.close();
    }

}
