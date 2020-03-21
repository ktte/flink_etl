package cn.ktte.shop.realtime.etl.process

import cn.itcast.canal.bean.RowData
import cn.itcast.shop.realtime.etl.bean.OrderDBEntity
import cn.itcast.shop.realtime.etl.utils.{GlobalConfigUtil, KafkaProps}
import cn.ktte.shop.realtime.etl.bean.OrderDBEntity
import cn.ktte.shop.realtime.etl.process.base.MySQLBaseETL
import cn.ktte.shop.realtime.etl.utils.GlobalConfigUtil
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema

/**
 * 订单的ETL处理类
 *
 * @param env
 */
class OrderETL(env: StreamExecutionEnvironment) extends MySQLBaseETL(env) {
  //业务处理的方法
  override def process(): Unit = {
    //获取数据源
    val source: DataStream[RowData] = getDataSource()
    //    1. 编写订单的实体类对象:`OrderDBEntity`
    //2. 对数据进行过滤操作:
    val orderStream: DataStream[String] = source
      //   1. 我们只要订单表数据:`itcast_orders`
      .filter(_.getTableName == "itcast_orders")
      //   2. 我们只要新增的数据: `insert`
      //      下完订单后,能改订单吗?比如订单的状态.比如下完订单后,订单的状态从已下单->已支付->已发货....
      //      如果同一笔订单,我们只需要记录`insert`插入事件即可.不需要关注后面的状态变更.
      .filter(_.getEventType == "insert")
      //3. 将RowData数据 => OrderDBEntity
      .map(OrderDBEntity(_))
      //4. 将OrderDBEntity=> JSON
      // fastJSON在scala中,如果是直接将一个对象转换为字符串有BUG,我们需要关闭一个循环调用的开关.
      .map(order => JSON.toJSONString(order, SerializerFeature.DisableCircularReferenceDetect))
    //打印数据
    orderStream.printToErr("订单数据: ")

    orderStream
    //5. 将数据发送给Kafka
      .addSink(send2Kafka(GlobalConfigUtil.`output.topic.order`))
  }
}