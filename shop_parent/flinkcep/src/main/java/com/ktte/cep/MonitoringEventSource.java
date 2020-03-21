package com.itheima.cep;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 我们通过自定义的source来模拟生成MonitoringEvent数据。
 */
public class MonitoringEventSource extends RichParallelSourceFunction<MonitoringEvent> {
    private boolean running = true;
    //最大机架id
    private final int maxRackId = 10;
    //暂停
    private final long pause = 100;
    //温度阈值 0.5
    private final double temperatureRatio = 0.5;
    //标准功率
    private final double powerStd = 100;
    //平均功率
    private final double powerMean = 10;
    //标准温度
    private final double temperatureStd = 80;
    //平均温度
    private final double temperatureMean = 20;
    //碎片
    private int shard;
    //偏移量
    private int offset;
    public MonitoringEventSource() { }

    @Override
    public void open(Configuration configuration) {
        //numParallelSubtasks 表示总共并行的subtask 的个数，
        int numberTasks = getRuntimeContext().getNumberOfParallelSubtasks();
        //获取当前子任务的索引
        int index = getRuntimeContext().getIndexOfThisSubtask();
        offset = (int) ((double) maxRackId / numberTasks * index);
        shard = (int) ((double) maxRackId / numberTasks * (index + 1)) - offset;
        System.out.println("numberTasks:"+numberTasks+", index:"+index+", offset:"+offset+", shard:"+shard);
    }
    public void run(SourceContext<MonitoringEvent> sourceContext) throws Exception {
        while (running) {
            //监视事件对象
            MonitoringEvent monitoringEvent;
            //多线程下生成随机数的对象
            final ThreadLocalRandom random = ThreadLocalRandom.current();
            int rackId = random.nextInt(shard) + offset;
            //如果生成的随机温度大于温度阈值，那么就是过热
            if (random.nextDouble() >= temperatureRatio) {
                //用Random类中的nextGaussian()方法，可以产生服从高斯分布的随机数，高斯分布即标准正态分布，均值为0，方差为1。
                double power = random.nextGaussian() * powerStd + powerMean;
                monitoringEvent = new PowerEvent(rackId, power, System.currentTimeMillis());
            } else {
                double temperature = random.nextGaussian() * temperatureStd + temperatureMean;
                monitoringEvent = new TemperatureEvent(rackId, temperature, System.currentTimeMillis());
            }
            sourceContext.collect(monitoringEvent);
            Thread.sleep(pause);
        }
    }

    //volatile
    /**
     * 1：保证可见性
     * 2：不保证原子性
     * 3：禁止指令重排
     *
     * JVM：主内存、工作内存
     */
    public void cancel() {
        running = false;
    }
}