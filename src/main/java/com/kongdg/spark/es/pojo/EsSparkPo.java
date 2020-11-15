package com.kongdg.spark.es.pojo;

import java.io.Serializable;

/**
 * @author userkdg
 * @date 2020-04-06 22:59
 **/
public class EsSparkPo implements Serializable {
    private static final long serialVersionUID = 1L;
    private String OTP,SFO,arrival,departure,id,one,two;

    public String getOTP() {
        return OTP;
    }

    public void setOTP(String OTP) {
        this.OTP = OTP;
    }

    public String getSFO() {
        return SFO;
    }

    public void setSFO(String SFO) {
        this.SFO = SFO;
    }

    public String getArrival() {
        return arrival;
    }

    public void setArrival(String arrival) {
        this.arrival = arrival;
    }

    public String getDeparture() {
        return departure;
    }

    public void setDeparture(String departure) {
        this.departure = departure;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOne() {
        return one;
    }

    public void setOne(String one) {
        this.one = one;
    }

    public String getTwo() {
        return two;
    }

    public void setTwo(String two) {
        this.two = two;
    }
}
