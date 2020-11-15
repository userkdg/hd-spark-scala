package com.kongdg.spark.base;

/**
 * @author userkdg
 * @date 2020-04-06 10:39
 **/
public abstract class BaseInit {
    static {
        //        为了满足hadoop在windows上有权限操作 winutils.exe 否则会导致写入文件等报权限不足
        String property = System.getProperty("os.name");
        if (property.toLowerCase().contains("windows")) {
            System.setProperty("hadoop.home.dir", "D:\\bigdata\\hadoop-2.7.2");
        } else {
            System.out.println("非windows本地模式无需加载hadoop的winutils.exe");
        }
    }

}
