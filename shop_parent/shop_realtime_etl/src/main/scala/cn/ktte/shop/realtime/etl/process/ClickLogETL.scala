package cn.ktte.shop.realtime.etl.process

import java.io.File

import cn.itcast.shop.realtime.etl.bean.ip.IPSeeker
import cn.itcast.shop.realtime.etl.bean.{ClickLogBean, ClickLogWideBean}
import cn.itcast.shop.realtime.etl.process.base.MQBaseETL
import cn.itcast.shop.realtime.etl.utils.{GlobalConfigUtil, KafkaProps}
import cn.ktte.shop.realtime.etl.bean.{ClickLogBean, ClickLogWideBean}
import cn.ktte.shop.realtime.etl.bean.ip.IPSeeker
import cn.ktte.shop.realtime.etl.process.base.MQBaseETL
import cn.ktte.shop.realtime.etl.utils.GlobalConfigUtil
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
 * 点击流日志的处理类
 */
class ClickLogETL(env: StreamExecutionEnvironment) extends MQBaseETL(env){
  override def process(): Unit = {
    // 获取点击流日志的数据源
    val clickSource: DataStream[String] = getDataSource(GlobalConfigUtil.`input.topic.click_log`)
      //    1. 使用Httpd将日志信息 => ClickLogBean
      //map操作有2种方式
      //1. 使用函数式, 这种方式优点是简洁,缺点是如果需要自定义的一些操作,不够灵活
      //2. 使用对象, 优点:灵活,缺点: 代码臃肿.
      .map(new RichMapFunction[String, ClickLogBean] {
        //定义解析器
        var parser: HttpdLoglineParser[ClickLogBean] = _

        // open方法只有在线程开启的时候调用1次,不会重复调用.
        override def open(parameters: Configuration): Unit = {
          // 创建解析器
          parser = ClickLogBean.createClickLogParser()
        }

        // 我们要执行的转换动作.
        override def map(logStr: String): ClickLogBean = {
          //开始解析
          parser.parse(logStr)
        }
      })
      //测试
      //      .print()
      //2. 对ClickLogBean的地域信息进行拉宽操作. ClickLogBean => ClickLogWideBean(有地域信息)
      .map(new RichMapFunction[ClickLogBean, ClickLogWideBean] {
        //定义一个扫描器
        var seeker: IPSeeker = _

        override def open(parameters: Configuration): Unit = {
          // 获取分布式缓存数据
          val file: File = getRuntimeContext.getDistributedCache.getFile("qqwry.dat")
          //创建IP扫描器
          seeker = new IPSeeker(file)
        }

        override def close(): Unit = {

        }

        override def map(in: ClickLogBean): ClickLogWideBean = {
          // 将ClickLogBean => ClickLogWideBean
          val wideBean: ClickLogWideBean = ClickLogWideBean(in)
          //补充城市/省份/时间信息.
          //我们可以用省进行切割,但是因为直辖市没有省份的概念,所以我们应该单独处理
          val country: String = seeker.getCountry(wideBean.getIp)
          //定义省份和城市
          var province: String = ""
          var city: String = ""
          val arr: Array[String] = country.split("省")
          if (arr.length > 1) {
            //有省份
            //河南省郑州市
            province = arr(0) + "省"
            city = arr(1)
          } else {
            //直辖市
            //上海市
            val temArr: Array[String] = country.split("市")
            // 长度校验此处忽略
            province = temArr(0)
            city = country
          }
          //将省份和城市赋值给宽表
          wideBean.setProvince(province)
          wideBean.setCity(city)
          //用当前时间作为时间戳,如果有需要,也可以用时间格式化将数据中的time_local格式化为时间戳.
          wideBean.setTimestamp(System.currentTimeMillis())
          println(s"省份: ${province} 城市: ${city}")
          wideBean
        }
      })
      //3. 将ClickLogWideBean => JSON字符串
      .map(line => JSON.toJSONString(line, SerializerFeature.DisableCircularReferenceDetect))
    //添加打印功能
    clickSource.printToErr("点击日志数据:")

    clickSource
    //4. 将json发送到Kafka的DWD层:`dwd_click_log`
      .addSink(send2Kafka(GlobalConfigUtil.`output.topic.clicklog`))
  }
}


















