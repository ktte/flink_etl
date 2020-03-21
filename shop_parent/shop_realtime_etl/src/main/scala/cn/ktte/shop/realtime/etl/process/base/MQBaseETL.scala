package cn.ktte.shop.realtime.etl.process.base

import java.util.Properties

import cn.itcast.shop.realtime.etl.utils.KafkaProps
import cn.ktte.shop.realtime.etl.utils.KafkaProps
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
 * String类型的ETL操作的通用类
 */
abstract class MQBaseETL(env: StreamExecutionEnvironment) extends BaseETL[String] {
  /**
   * 获取数据源
   *
   * @param topic 需要消费的Topic名称
   * @return
   */
  override def getDataSource(topic: String): DataStream[String] = {
  // 获取Kafka的消费对象(数据源)
    val kafkaConsumer = new FlinkKafkaConsumer011[String](
      topic,
      new SimpleStringSchema(),
      KafkaProps.getKafkaProps()
    )
    //获取Kafka数据源.
    val sourceStream: DataStream[String] = env.addSource(kafkaConsumer)
    //返回数据源
    sourceStream
  }
}
