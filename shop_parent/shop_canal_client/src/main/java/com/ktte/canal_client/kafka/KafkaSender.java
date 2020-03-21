package com.ktte.canal_client.kafka;

import cn.ktte.canal.bean.RowData;
import com.ktte.canal_client.util.ConfigUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Kafka的发送者
 */
public class KafkaSender {
    public static KafkaProducer<String, RowData> kafkaProducer;

    static {
        // 给kafkaProducer进行初始化.
        Properties properties = new Properties();
        //设置properties相关的参数,连接的地址.序列化...
        properties.setProperty("bootstrap.servers", ConfigUtil.KAFKA_BOOTSTRAP_SERVERS_CONFIG);
        properties.setProperty("batch.size", ConfigUtil.KAFKA_BATCH_SIZE_CONFIG + "");
        properties.setProperty("acks", ConfigUtil.KAFKA_ACKS);
        properties.setProperty("retries", ConfigUtil.KAFKA_RETRIES + "");
        properties.setProperty("key.serializer", ConfigUtil.KAFKA_KEY_SERIALIZER_CLASS_CONFIG);
        properties.setProperty("value.serializer", ConfigUtil.KAFKA_VALUE_SERIALIZER_CLASS_CONFIG);
        kafkaProducer = new KafkaProducer<String, RowData>(properties);
    }

    /**
     * 负责将数据发送给Kafka
     * @param message
     */
    public static void send(RowData message){
        kafkaProducer.send(new ProducerRecord(ConfigUtil.KAFKA_TOPIC, message));
    }



}
