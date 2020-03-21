package com.itheima.cep;

public class TemperatureWarning {

    //机架id
    private int rackID;
    //平均温度
    private double averageTemperature;

    public TemperatureWarning(int rackID, double averageTemperature) {
        this.rackID = rackID;
        this.averageTemperature = averageTemperature;
    }

    public TemperatureWarning() {
        this(-1, -1);
    }

    public int getRackID() {
        return rackID;
    }

    public void setRackID(int rackID) {
        this.rackID = rackID;
    }

    public double getAverageTemperature() {
        return averageTemperature;
    }

    public void setAverageTemperature(double averageTemperature) {
        this.averageTemperature = averageTemperature;
    }

    @Override
    public String toString() {
        return "TemperatureWarning(" + getRackID() + ", " + averageTemperature + ")";
    }
}