package com.itheima.cep;

public class PowerEvent extends MonitoringEvent {
    //电压
    private double voltage;
    //时间
    private long timestamp;

    public PowerEvent(int rackID, double voltage, long timestamp) {
        super(rackID);

        this.voltage = voltage;
        this.timestamp = timestamp;
    }

    public void setVoltage(double voltage) {
        this.voltage = voltage;
    }

    public double getVoltage() {
        return voltage;
    }

    @Override
    public String toString() {
        return "PowerEvent(" + getRackID() + ", " + voltage + ", " + timestamp + ")";
    }
}