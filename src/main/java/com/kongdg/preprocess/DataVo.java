package com.kongdg.preprocess;

import com.clearspring.analytics.util.Lists;
import org.bson.Document;

import java.util.List;

/**
 * @author userkdg
 * @date 2020-04-11 22:29
 **/
public class DataVo {
    public static void main(String[] args) {
        // qq 与imsi关联关系:
        // qq =>实名信息=>通联关系=>三码表
        // qq => 手机号码=> 三码表

        //qq与
        List<Document> doc1 = Lists.newArrayList();
        doc1.add(new Document().append("aist","1265598442").append("mt","qq"));
        doc1.add(new Document().append("aist","126559844a").append("mt","weixin"));

        List<Document> doc2 = Lists.newArrayList();
        doc2.add(new Document().append("imsi","126559xx").append("mt","qq"));
        doc2.add(new Document().append("imsi","126559844a").append("mt","weixin"));

    }
}
