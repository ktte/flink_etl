package cn.ktte.cep

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 检查用户恶意刷屏信息
 */
object CheckUserInputDemo2 {

  case class Message(userId: String, ip: String, msg: String)

  def main(args: Array[String]): Unit = {
    //1. 获取流处理运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2. 设置并行度为1
    env.setParallelism(1)

    //因为当前消息上,没有时间,所以我们就直接以事件发生时间作为时间的处理方式.

    // 模拟数据源
    val loginEventStream: DataStream[Message] = env.fromCollection(
      List(
        Message("1", "192.168.0.1", "beijing"),
        Message("1", "192.168.0.2", "beijing"),
        Message("1", "192.168.0.3", "beijing"),
        Message("1", "192.168.0.4", "beijing"),
        Message("2", "192.168.10.10", "shanghai"),
        Message("3", "192.168.10.10", "beijing"),
        Message("3", "192.168.10.11", "beijing"),
        Message("4", "192.168.10.10", "beijing"),
        Message("5", "192.168.10.11", "shanghai"),
        Message("4", "192.168.10.12", "beijing"),
        Message("5", "192.168.10.13", "shanghai"),
        Message("5", "192.168.10.14", "shanghai"),
        Message("5", "192.168.10.15", "beijing"),
        Message("6", "192.168.10.16", "beijing"),
        Message("6", "192.168.10.17", "beijing"),
        Message("6", "192.168.10.18", "beijing"),
        Message("5", "192.168.10.18", "shanghai"),
        Message("6", "192.168.10.19", "beijing"),
        Message("6", "192.168.10.19", "beijing"),
        Message("5", "192.168.10.18", "shanghai")
      )
    )

    //指定规则
    //    - 用户如果在10s内，同时连续输入同样一句话超过5次，就认为是恶意刷屏
    //- 使用 Flink CEP检测刷屏用户
    val pattern: Pattern[Message, Message] = Pattern.begin[Message]("begin")
      .where(_.msg != null)
      .where(_.msg != "")
      .times(5)
      .within(Time.seconds(10))

    //使用CEP整合数据流和规则
    val result: DataStream[Message] = CEP.pattern(loginEventStream.keyBy(_.userId), pattern)
      .select(
        map => {
          //将用户输入的信息获取出来
          val list: List[Message] = map.getOrElse("begin", List()).toList
          println("用户连续说了5句话: " + list)
          //看这些信息是否是同一句话
          //我们需要将List中的msg获取出来,之后将msg进行去重操作,如果去重之后的长度变为1,肯定是同一句话.
          val distinctList: List[String] = list.map(_.msg).distinct
          if (distinctList.size == 1) {
            //肯定是同一句话, 返回最后一次说的话
            list.last
          } else {
            //不是同一句话, 返回值不要写为null,因为后面后报空指针.
            Message("", "", "")
          }
        }
      )
    //获取数据
    result.filter(_.msg != "").printToErr("恶意刷屏的用户: ")

    env.execute()

  }
}
