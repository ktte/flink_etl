package cn.ktte.shop.realtime.cep

import cn.itcast.canal.bean.RowData
import cn.itcast.shop.realtime.util.GlobalConfigUtil
import cn.ktte.shop.realtime.bean.OrderDBEntity
import cn.ktte.shop.realtime.util.GlobalConfigUtil
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 订单的ETL处理类
 *
 * @param env
 */
class LateOrderCEP(env: StreamExecutionEnvironment) extends MySQLBaseETL(env) {
  //业务处理的方法
  override def process(): Unit = {
    //获取数据源
    val source: DataStream[RowData] = getDataSource()
    //    1. 编写订单的实体类对象:`OrderDBEntity`
    //2. 对数据进行过滤操作:
    val orderStream: DataStream[OrderDBEntity] = source
      //   1. 我们只要订单表数据:`itcast_orders`
      .filter(_.getTableName == "itcast_orders")
      //3. 将RowData数据 => OrderDBEntity
      .map(OrderDBEntity(_))

    //定义订单的超时规则信息
    val pattern: Pattern[OrderDBEntity, OrderDBEntity] = Pattern.begin[OrderDBEntity]("first")
      // 找到已经创建成功的订单 订单状态 -3:用户拒收 -2:未付款的订单 -1：用户取消 0:待发货 1:配送中 2:用户确认收货
      .where(_.orderStatus == -2) //用户已下单,未付款
      //看订单是否是已经支付(可以不挨着)
      .followedBy("second")
      .where(_.orderStatus == 0)
      //必须在15分钟之内
      .within(Time.minutes(15))

    //将数据流和规则进行匹配
    val patternStream: PatternStream[OrderDBEntity] = CEP.pattern(orderStream.keyBy(_.orderId), pattern)
    //获取匹配的结果信息.
    //侧道输出: 解决数据的延迟到达.
    //定义侧道输出标记
    val outTag = new OutputTag[OrderDBEntity]("outTag")
    //选择数据
    val successStream: DataStream[OrderDBEntity] = patternStream
      //指定侧道输出对象
      .select[OrderDBEntity, OrderDBEntity](outTag)(
        (timeOutMap: collection.Map[String, Iterable[OrderDBEntity]], time: Long) => {
          //获取超时的数据,将订单对象转换为超时对象
          val lateList: List[OrderDBEntity] = timeOutMap.getOrElse("first", List()).toList
          //因为当前数据中都是同一笔订单信息,所以我们取第几个是无所谓的
          val orderEvent: OrderDBEntity = lateList.head
          orderEvent
        })((selectMap: collection.Map[String, Iterable[OrderDBEntity]]) => {
        //获取正常的数据,没有超时的订单信息
        val orderList: List[OrderDBEntity] = selectMap.getOrElse("first", List()).toList
        orderList.head
      })
    //输出正常的订单
    successStream.print("正常支付的订单信息: ")
    //输出超时的订单.
    val lateStream: DataStream[OrderDBEntity] = successStream.getSideOutput(outTag)
    lateStream.printToErr("超出订单的支付时间: ")

    //将延迟数据转换为JSON,并发送给Kafka.
    lateStream.map(order => JSON.toJSONString(order, SerializerFeature.DisableCircularReferenceDetect))
      //5. 将数据发送给Kafka
      .addSink(send2Kafka(GlobalConfigUtil.`output.topic.order`))
  }
}