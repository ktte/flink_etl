package cn.ktte.cep

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 检查用户恶意攻击案例
 */
object CheckUserInputDemo1 {
  case class LoginEvent(userId: String, msg: String, timestamp: Long)

  def main(args: Array[String]): Unit = {
    //1. 获取流处理运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2. 设置并行度为1
    env.setParallelism(1)
    //3. 设置流处理的时间特性(重要)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //加载数据源
    // 模拟数据源
    val loginEventStream: DataStream[LoginEvent] = env.fromCollection(
      List(
        LoginEvent("1", "TMD", 1558430842),
        LoginEvent("1", "TMD", 1558430843),
        LoginEvent("1", "TMD", 1558430845),
        LoginEvent("1", "TMD", 1558430850),
        LoginEvent("2", "TMD", 1558430851),
        LoginEvent("1", "TMD", 1558430851)
      )
    ).assignAscendingTimestamps(_.timestamp * 1000)
    //对用户进行分组
    val groupStream: KeyedStream[LoginEvent, String] = loginEventStream.keyBy(_.userId)

    //定义规则
    //用户如果在10s内，同时输入TMD 超过5次，就认为用户为恶意攻击，识别出该用户
    val pattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin")
      .where(_.msg == "TMD")
      .times(5) //指定次数为5次
      .within(Time.seconds(10))

    //使用CEP对数据进行匹配
    val result: DataStream[LoginEvent] = CEP.pattern(groupStream, pattern)
      .select(
        (map: collection.Map[String, Iterable[LoginEvent]]) => {
          //能进来的数据肯定是匹配上来了,那么我们直接从Map中取出一个数据返回即可.
          val list: List[LoginEvent] = map.getOrElse("begin", List()).toList //如果没有取到数据,返回一个空的list
          println(list)
          //返回集合数据的最后一条信息
          list.last
        }
      )


    //找到恶意攻击的用户
    result.printToErr("恶意攻击的用户: ")
    env.execute()

  }
}
