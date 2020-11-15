import com.google.common.collect.Maps;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;

/**
 * @author userkdg
 * @date 2019/7/28 22:38
 **/
public class CsvHandler {

    public static void main(String[] args) {
        CsvHandler read = new CsvHandler();
        read.run();
    }

    public void run() {
        String csv = "F:\\孔德刚\\测试csv行读取.csv";
        BufferedReader br;
        String line;
        String csvSplitBy = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
        //where should we save
        try {
            br = new BufferedReader(new FileReader(csv));

            int firstRow = 0;
            String[] csvHeader = new String[0];
            while ((line = br.readLine()) != null) {
                //use comma as separatpr
                String[] major = line.split(csvSplitBy);
                // 首行字段
                if (firstRow++ == 0) {
                    csvHeader = major.clone();
                    continue;
                }
                if (major.length != csvHeader.length) {
                    System.err.println("表头和列数不一致，跳过处理");
                } else {
                    LinkedHashMap<String, String> fieldAndValueMap = Maps.newLinkedHashMap();
                    for (int i = 0; i < major.length; i++) {
                        fieldAndValueMap.put(csvHeader[i], major[i]);
                    }
                    System.out.println(fieldAndValueMap);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
