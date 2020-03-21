package cn.ktte.cep

import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 检测用户的订单支付超时时间,找到15分钟未支付的订单信息
 */
object OrderPayDemo {

  //订单对象
  case class OrderEvent(userId: Int, status: String, timeStamp: Long)

  //延迟的订单对象
  case class LateOrder(id: Int, timeStamp: Long)

  def main(args: Array[String]): Unit = {
    //1. 获取流处理运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2. 设置并行度为1
    env.setParallelism(1)
    //3. 设置流处理的时间特性(重要)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //加载数据源
    val orderSource: DataStream[OrderEvent] = env.fromCollection(
      List(
        OrderEvent(1, "create", 1558430842), //2019-05-21 17:27:22
        OrderEvent(2, "create", 1558430843), //2019-05-21 17:27:23
        OrderEvent(2, "other", 1558430845), //2019-05-21 17:27:25
        OrderEvent(2, "pay", 1558430850), //2019-05-21 17:27:30
        OrderEvent(1, "pay", 1558431920) //2019-05-21 17:45:20
      )
    ).assignAscendingTimestamps(_.timeStamp * 1000)


    //用户下单以后，应该设置订单失效时间，用来提高用户的支付意愿,
    // 如果用户下单15分钟未支付，则输出监控信息

    //定义订单超时的规则
    val pattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("first")
      // 找到已经创建成功的订单
      .where(_.status == "create")
      //看订单是否是已经支付(可以不挨着)
      .followedBy("second")
      .where(_.status == "pay")
      //必须在15分钟之内
      .within(Time.minutes(15))

    //将规则和数据流进行绑定
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderSource.keyBy(_.userId), pattern)

    //侧道输出: 解决数据的延迟到达.
    //定义侧道输出标记
    val outTag = new OutputTag[LateOrder]("outTag")
    //选择数据
    val successStream: DataStream[OrderEvent] = patternStream
      //指定侧道输出对象
      .select[LateOrder, OrderEvent](outTag)(
        (timeOutMap: collection.Map[String, Iterable[OrderEvent]], time: Long) => {
          //获取超时的数据,将订单对象转换为超时对象
          val lateList: List[OrderEvent] = timeOutMap.getOrElse("first", List()).toList
          //因为当前数据中都是同一笔订单信息,所以我们取第几个是无所谓的
          val orderEvent: OrderEvent = lateList.head
          //将订单转换为超时对象
          LateOrder(orderEvent.userId, orderEvent.timeStamp)
        })((selectMap: collection.Map[String, Iterable[OrderEvent]]) => {
        //获取正常的数据,没有超时的订单信息
        val orderList: List[OrderEvent] = selectMap.getOrElse("first", List()).toList
        orderList.head
      })
    //输出正常的订单
    successStream.print("正常支付的订单信息: ")
    //输出超时的订单.
    val lateStream: DataStream[LateOrder] = successStream.getSideOutput(outTag)
    lateStream.printToErr("超出订单的支付时间: ")
    env.execute()

  }
}
