package com.itheima.cep;

/**
 * 首先我们定义一个监控事件
 */
public abstract class MonitoringEvent {
    private int rackID;

    public MonitoringEvent(int rackID) {
        this.rackID = rackID;
    }

    public int getRackID() {
        return rackID;
    }

    public void setRackID(int rackID) {
        this.rackID = rackID;
    }
}