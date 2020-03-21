package cn.ktte.canal.protobuf;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class ProtoBufSerializer implements Serializer<ProtoBufable> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //自定义的配置
    }

    /**
     * 实现向Kafka中发送消息的核心方法
     * @param topic
     * @param data 本次要发送的数据
     * @return
     */
    @Override
    public byte[] serialize(String topic, ProtoBufable data) {
        return data.toByte();
    }

    @Override
    public void close() {
        //目前不需要
    }
}
