package cn.ktte.monitor;

import com.itheima.cep.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * * 输入事件流由来自一组机架的温度和功率事件组成
 * * 目标是检测当机架过热时我们需要发出警告和报警
 * * 警告：某机架在10秒内连续两次上报的温度超过阈值
 * * 报警：某机架在20秒内连续两次匹配警告，并且第二次的温度超过了第一次的温度就告警
 */
public class SystemMonitoring {
    //设置默认的温度最大值为100度
    private static final double MAX_TEMPERATURE = 100.0;
    public static void main(String[] args) throws Exception {
        // 1. 初始化运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        //2. 设置时间的处理策略,事件发生时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //3. 接入数据源,MonitoringEvent
        SingleOutputStreamOperator<MonitoringEvent> source = env.addSource(new MonitoringEventSource())
                // 使用数据到达的时间作为时间戳.
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());
//        source.print();

        //可以添加filter将温度相关的数据过滤出来

        //4. 定义警告的模式规则  某机架在10秒内连续两次上报的温度超过阈值
        Pattern<MonitoringEvent, TemperatureEvent> warnningPattern = Pattern.<MonitoringEvent>begin("first")
                //指定当前的数据类型必须为TemperatureEvent
                .subtype(TemperatureEvent.class)
                .where(new IterativeCondition<TemperatureEvent>() {
                    @Override
                    public boolean filter(TemperatureEvent temp, Context<TemperatureEvent> ctx) throws Exception {
                        //如果数据是我们需要的,返回true
                        //看温度有没有超出阈值.
                        return temp.getTemperature() > MAX_TEMPERATURE;
                    }
                })
                .next("second")
                .subtype(TemperatureEvent.class)
                .where(new IterativeCondition<TemperatureEvent>() {
                    @Override
                    public boolean filter(TemperatureEvent temp, Context<TemperatureEvent> ctx) throws Exception {
                        //如果数据是我们需要的,返回true
                        //看温度有没有超出阈值.
                        return temp.getTemperature() > MAX_TEMPERATURE;
                    }
                })
                .within(Time.seconds(10));
        //5. 将警告的规则应用到数据流中
        PatternStream<MonitoringEvent> warningPatternStream = CEP.pattern(source.keyBy("rackID"), warnningPattern);
        //6. 获取匹配结果.匹配到的都是警告数据
        SingleOutputStreamOperator<TemperatureWarning> warningStream = warningPatternStream.select(new PatternSelectFunction<MonitoringEvent, TemperatureWarning>() {
            @Override
            public TemperatureWarning select(Map<String, List<MonitoringEvent>> pattern) throws Exception {
                //获取匹配到的数据,拿第一个数据和第二个数据的温度值
                TemperatureEvent firstEvent = (TemperatureEvent) pattern.get("first").get(0);
                TemperatureEvent secondEvent = (TemperatureEvent) pattern.get("second").get(0);
                //返回平均温度
                double avgTmp = (firstEvent.getTemperature() + secondEvent.getTemperature()) / 2;
                return new TemperatureWarning(firstEvent.getRackID(), avgTmp);
            }
        });
        //打印符合警告条件的数据
        warningStream.print("触发警告的信息: ");
        //7. 定义告警匹配规则 某机架在20秒内连续两次匹配警告，并且第二次的温度超过了第一次的温度就告警
        Pattern<TemperatureWarning, TemperatureWarning> alertPattern = Pattern
                .<TemperatureWarning>begin("first")
                .next("second")
                .within(Time.seconds(20));
        //8. 将告警的规则应用到警告的数据流中
        PatternStream<TemperatureWarning> alertPatternStream = CEP.pattern(warningStream.keyBy("rackID"), alertPattern);
        //9. 获取告警的数据
        SingleOutputStreamOperator<TemperatureAlert> alertStream = alertPatternStream.select(new PatternSelectFunction<TemperatureWarning, TemperatureAlert>() {
            @Override
            public TemperatureAlert select(Map<String, List<TemperatureWarning>> pattern) throws Exception {
                //获取第二次和第一次的数据,看第二次温度有没有超出第一次的温度.
                TemperatureWarning first = pattern.get("first").get(0);
                TemperatureWarning second = pattern.get("second").get(0);
                //判断温度
                if (second.getAverageTemperature() > first.getAverageTemperature()) {
                    //触发告警,返回一个告警的对象
                    return new TemperatureAlert(first.getRackID());
                } else {
                    // 使用无参构造,返回的机架ID为-1
                    return new TemperatureAlert();
                }
            }
        });
        //10. 打印数据
        //先过滤出机架ID为-1的数据
        SingleOutputStreamOperator<TemperatureAlert> filterStream = alertStream.filter(new FilterFunction<TemperatureAlert>() {
            @Override
            public boolean filter(TemperatureAlert temperatureAlert) throws Exception {
                return temperatureAlert.getRackID() != -1;
            }
        });
        filterStream.printToErr("告警信息>>>>>:");

        //11. 执行程序
        env.execute("SystemMonitoring");

    }
}
