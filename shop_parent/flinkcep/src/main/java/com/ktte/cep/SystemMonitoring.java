package com.itheima.cep;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * 需求：
 * - 输入事件流由来自一组机架的温度（TemperatureEvent）和功率（PowerEvent）事件组成
 * - 目标是检测当机架过热时我们需要发出警告和报警
 *   - 警告：某机架在10秒内连续两次上报的温度超过阈值
 *   - 报警：某机架在20秒内连续两次匹配警告，并且第二次的温度超过了第一次的温度就告警
 */
public class SystemMonitoring {
    //定义温度的阈值
    private static final double TEMPERATURE_THEWHOLD = 100;

    public static void main(String[] args) throws Exception {
        //1：初始化运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2：设置按照事件时间来处理数据
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //3：接入数据源
        DataStream<MonitoringEvent> monitoringEventDataStreamSource = env.addSource(new MonitoringEventSource())
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

        //4：实现业务处理
        /**
         * 警告：某机架在10秒内连续两次上报的温度超过阈值
         * 4.1  定义模式规则
         * 4.2  将模式规则应用到数据流中
         * 4.3  获取匹配到的数据
         */

        //4.1  定义模式规则
        Pattern<MonitoringEvent, TemperatureEvent> waringPattern = Pattern.<MonitoringEvent>begin("first").subtype(TemperatureEvent.class).where(new IterativeCondition<TemperatureEvent>() {
            @Override
            public boolean filter(TemperatureEvent temperatureEvent, Context<TemperatureEvent> context) throws Exception {
                return temperatureEvent.getTemperature() > TEMPERATURE_THEWHOLD;
            }
        }).next("second").subtype(TemperatureEvent.class).where(new IterativeCondition<TemperatureEvent>() {
            @Override
            public boolean filter(TemperatureEvent temperatureEvent, Context<TemperatureEvent> context) throws Exception {
                return temperatureEvent.getTemperature() > TEMPERATURE_THEWHOLD;
            }
        }).within(Time.seconds(10));

        //4.2  将模式规则应用到数据流中
        PatternStream<MonitoringEvent> warningPatternStream = CEP.pattern(monitoringEventDataStreamSource.keyBy("rackID"), waringPattern);

        //4.3  获取匹配到的数据
        SingleOutputStreamOperator<TemperatureWarning> warning = warningPatternStream.select(new PatternSelectFunction<MonitoringEvent, TemperatureWarning>() {
            @Override
            public TemperatureWarning select(Map<String, List<MonitoringEvent>> pattern) throws Exception {
                //找到匹配上的两次温度事件
                TemperatureEvent first = (TemperatureEvent) pattern.get("first").get(0);
                TemperatureEvent second = (TemperatureEvent) pattern.get("second").get(0);

                //返回警告的数据对象
                return new TemperatureWarning(first.getRackID(), (first.getTemperature() + second.getTemperature()) / 2);
            }
        });

        //打印符合条件的警告流数据
        warning.print("警告信息>>>");

        /**
         * 5：业务处理
         * 报警：某机架在20秒内连续两次匹配警告，并且第二次的温度超过了第一次的温度就告警
         **/
        //5.1：定义告警规则
        Pattern<TemperatureWarning, TemperatureWarning> alterPattern = Pattern.<TemperatureWarning>begin("first").next("second").within(Time.seconds(20));

        //5.2：将告警规则应用到警告流中
        PatternStream<TemperatureWarning> alterPatternStream = CEP.pattern(warning.keyBy("rackID"), alterPattern);

        //5.3：获取到告警的数据
        SingleOutputStreamOperator<TemperatureAlert> alter = alterPatternStream.select(new PatternSelectFunction<TemperatureWarning, TemperatureAlert>() {
            @Override
            public TemperatureAlert select(Map<String, List<TemperatureWarning>> pattern) throws Exception {
                //找到匹配上的两次温度事件
                TemperatureWarning first = (TemperatureWarning) pattern.get("first").get(0);
                TemperatureWarning second = (TemperatureWarning) pattern.get("second").get(0);

                //第二次警告的温度大于第一次警告的温度
                if (second.getAverageTemperature() > first.getAverageTemperature()) {
                    //返回告警实体对象
                    return new TemperatureAlert(first.getRackID());
                } else {
                    return null;
                }
            }
        });

        //打印告警数据
        alter.filter(new FilterFunction<TemperatureAlert>() {
            @Override
            public boolean filter(TemperatureAlert value) throws Exception {
                return value != null;
            }
        }).printToErr("告警信息>>>");

        //启动任务
        env.execute();
    }
}
