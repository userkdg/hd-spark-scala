package com.kongdg.spark.streaming;

import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

/**
 * @author userkdg
 * @date 2020/4/2 23:29
 **/
public class CloseStreamHanlder extends AbstractHandler {
    private JavaStreamingContext jsc;
    public CloseStreamHanlder(JavaStreamingContext jsc) {
        this.jsc = jsc;
    }

    @Override
    public void handle(String s, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, int i) throws IOException, ServletException {
        System.out.println("触发关闭...");
        SparkStreamDefineReceicer.isStart = false;
        try {
            TimeUnit.SECONDS.sleep(1L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//        SparkStreamDefineReceicer.closeBusData();
        jsc.stop(true,true);
        httpServletResponse.setCharacterEncoding("UTF-8");
        httpServletResponse.setContentType("text/html;charset=utf-8");
        httpServletResponse.setStatus(HttpServletResponse.SC_OK);
        try (PrintWriter writer = httpServletResponse.getWriter()) {
            writer.println("close success");
        }
    }
}
