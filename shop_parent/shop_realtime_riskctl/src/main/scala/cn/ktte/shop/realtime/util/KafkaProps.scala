package cn.ktte.shop.realtime.util

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig

/**
 * Kafka配置工具类
 */
object KafkaProps {

  def getKafkaProps(): Properties = {
    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, GlobalConfigUtil.`bootstrap.servers`)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GlobalConfigUtil.`group.id`)
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, GlobalConfigUtil.`enable.auto.commit`)
    properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, GlobalConfigUtil.`auto.commit.interval.ms`)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, GlobalConfigUtil.`auto.offset.reset`)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, GlobalConfigUtil.`key.deserializer`)
    properties
  }

}
