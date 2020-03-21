package com.itheima.cep;

public class TemperatureEvent extends MonitoringEvent {
    //温度
    private double temperature;
    //时间
    private Long timestamp;

    public TemperatureEvent(int rackID, double temperature, long timestamp) {
        super(rackID);

        this.temperature = temperature;
        this.timestamp = timestamp;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "TemperatureEvent(" + getRackID() + ", " + temperature + ", "+timestamp+")";
    }
}