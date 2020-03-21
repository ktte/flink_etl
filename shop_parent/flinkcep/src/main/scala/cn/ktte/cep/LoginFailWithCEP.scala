package cn.ktte.cep

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 使用FlinkCEP实现查找2秒内连续登录失败的用户信息
 */
object LoginFailWithCEP {

  //时间信息样例类 eventTime一定要设置为Long类型,否则会出现越界,数据就错.
  case class LoginEvent(id: Int, ip: String, status: String, eventTime: Long)

  def main(args: Array[String]): Unit = {
    //1. 获取流处理运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2. 设置并行度为1
    env.setParallelism(1)
    //3. 设置流处理的时间特性(重要)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //4. 加载数据源.并设置水印
    val loginEventStream = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842), // 1558430842 精确到秒,这不是一个毫秒值
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))
      //指定时间戳为单调递增.
      .assignAscendingTimestamps(_.eventTime * 1000)
    //分组
    val keyByStream: KeyedStream[LoginEvent, Int] = loginEventStream.keyBy(_.id)
    //5. 使用CEP进行数据处理,获取到连续登录失败的用户
    //使用FlinkCEP实现查找2秒内连续登录失败的用户信息
    // 条件:
    // 连续登录失败: 当前登录失败了,紧跟着又登录失败
    // 时间: 2秒内

    // 定义规则:(网)
    val pattern: Pattern[LoginEvent, LoginEvent] = Pattern
      // 定义规则的开始,需要指定泛型,告诉当前规则,我们要处理的数据是什么类型,参数: 当前规则叫什么名字
      .begin[LoginEvent]("begin")
      // 第一道网,找出第一次登录失败的用户
      .where(_.status == "fail")
      //定义下一次的规则
      .next("next")
      //第二道网
      .where(_.status == "fail")
      //第三道网: 时间2秒
      .within(Time.seconds(2))

    //使用pattern这个规则将数据流中的数据过滤出来
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(keyByStream, pattern)
    //从patternStream中获取我们需要的数据
    val resultStream: DataStream[LoginEvent] = patternStream.select(new PatternSelectFunction[LoginEvent, LoginEvent] {
      /**
       * 对数据进行选择,最终返回我们需要的数据
       *
       * @param patternMap 匹配到的数据
       * @return
       */
      override def select(patternMap: util.Map[String, util.List[LoginEvent]]): LoginEvent = {
        import collection.JavaConverters._
        //从Pattern中获取数据
        val beginList: List[LoginEvent] = patternMap.get("begin").asScala.toList
        val nextList: List[LoginEvent] = patternMap.get("next").asScala.toList

        println("===========开始=============")
        println(beginList)
        println("========================")
        println(nextList)
        println("===========结束=============")
        //返回数据
        nextList.head
      }
    })

    //6. 打印用户信息
    resultStream.printToErr("找到连续登录失败的用户")
    //7. 执行程序
    env.execute("LoginFailWithCEP")

  }
}
