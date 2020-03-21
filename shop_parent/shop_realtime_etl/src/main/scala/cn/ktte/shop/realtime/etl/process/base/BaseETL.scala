package cn.ktte.shop.realtime.etl.process.base

import cn.itcast.shop.realtime.etl.utils.KafkaProps
import cn.ktte.shop.realtime.etl.utils.KafkaProps
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
 * 所有ETL操作的基类,主要负责定义流程.
 * 比如定义获取数据源的方法/处理数据的方法..
 */
trait BaseETL[T] {

  /**
   * 获取数据源
   *
   * @param topic 需要消费的Topic名称
   * @return
   */
  def getDataSource(topic: String): DataStream[T]


  // 所有的ETL操作,方法名都必须为process
  def process()


  //定义一个Kafka的生产者(sink对象)
  def send2Kafka(topic: String): FlinkKafkaProducer011[String] = {
    val kafkaSink = new FlinkKafkaProducer011[String](
      topic,
      new SimpleStringSchema(),
      KafkaProps.getKafkaProps()
    )
    kafkaSink
  }

}
