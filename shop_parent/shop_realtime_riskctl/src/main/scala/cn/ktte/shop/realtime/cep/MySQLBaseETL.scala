package cn.ktte.shop.realtime.cep

import cn.itcast.canal.bean.RowData
import cn.itcast.shop.realtime.util.{CanalRowDataDeserializationSchema, GlobalConfigUtil}
import cn.ktte.shop.realtime.util.{CanalRowDataDeserializationSchema, GlobalConfigUtil, KafkaProps}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * 处理Canal相关数据的ETL父类
 */
abstract class MySQLBaseETL(env: StreamExecutionEnvironment) extends BaseETL[RowData]{
  /**
   * 获取数据源
   *
   * @param topic 需要消费的Topic名称, 默认值为: ods_itcast_shop_mysql
   * @return
   */
  override def getDataSource(topic: String = GlobalConfigUtil.`input.topic.canal`): DataStream[RowData] = {
    //获取Kafka中的Canal数据
    val canalConsumer = new FlinkKafkaConsumer011[RowData](
      topic,
      new CanalRowDataDeserializationSchema,
      KafkaProps.getKafkaProps()
    )
    val source: DataStream[RowData] = env.addSource(canalConsumer)
    //进行水印处理
    val waterStream: DataStream[RowData] = source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[RowData](Time.seconds(2)) {
      //指定时间列为SQL的执行时间
      override def extractTimestamp(element: RowData): Long = element.getExecuteTime
    })
    waterStream
  }
}
