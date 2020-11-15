package com.kongdg.spark.es.pojo;

import java.io.Serializable;

/**
 * @author userkdg
 * @date 2020-04-06 18:44
 **/
public
class TripBean implements Serializable {
    private static final long serialVersionUID = 1L;
    private String departure, arrival;

    public TripBean(String departure, String arrival) {
        setDeparture(departure);
        setArrival(arrival);
    }

    public TripBean() {}

    public String getDeparture() { return departure; }
    public String getArrival() { return arrival; }
    public void setDeparture(String dep) { departure = dep; }
    public void setArrival(String arr) { arrival = arr; }
}
