package cn.ktte.shop.realtime.etl.process

import java.sql.Timestamp
import java.util.Date

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import cn.itcast.shop.realtime.etl.bean.{Comments, CommentsWideEntity, DimGoodsDBEntity}
import cn.itcast.shop.realtime.etl.process.base.MQBaseETL
import cn.itcast.shop.realtime.etl.utils.{GlobalConfigUtil, RedisUtil}
import cn.ktte.shop.realtime.etl.bean.{Comments, CommentsWideEntity, DimGoodsDBEntity}
import cn.ktte.shop.realtime.etl.process.base.MQBaseETL
import cn.ktte.shop.realtime.etl.utils.{GlobalConfigUtil, RedisUtil}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

class CommentsETL(env: StreamExecutionEnvironment) extends MQBaseETL(env) {
  /**
   * 业务处理接口
   */
  override def process(): Unit = {
    // 1. 整合Kafka
    val commentsDS: DataStream[String] = getDataSource(GlobalConfigUtil.`input.topic.comments`)

    // 2. Flink实时ETL
    // 将JSON转换为实体类
    val commentsBeanDS: DataStream[Comments] = commentsDS.map(
      commentsJson=>{
        Comments(commentsJson)
      }
    )

    commentsBeanDS.printToErr("评论信息>>>")
    //将评论信息表进行拉宽操作
    val commentsWideBeanDataStream: DataStream[CommentsWideEntity] = commentsBeanDS.map(new RichMapFunction[Comments, CommentsWideEntity] {
      var jedis: Jedis = _

      override def open(parameters: Configuration): Unit = {
        jedis = RedisUtil.getJedis()
        jedis.select(1)
      }

      //释放资源
      override def close(): Unit = {
        if (jedis !=null && jedis.isConnected) {
          jedis.close()
        }
      }

      //对数据进行拉宽
      override def map(comments: Comments): CommentsWideEntity = {
        // 拉宽商品
        println("goodsJson start..."+comments.goodsId)
        val goodsJSON = jedis.hget("itcast_shop:dim_goods", comments.goodsId)
        println("goodsJson"+goodsJSON)
        val dimGoods = DimGoodsDBEntity(goodsJSON)

        //将时间戳转换为时间类型
        val timestamp = new Timestamp(comments.timestamp)
        val date = new Date(timestamp.getTime)

        CommentsWideEntity(
          comments.userId,
          comments.userName,
          comments.orderGoodsId,
          comments.starScore,
          comments.comments,
          comments.assetsViedoJSON,
          FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(date),
          comments.goodsId,
          dimGoods.goodsName,
          dimGoods.shopId
        )
      }
    })

    //3:将评论信息数据拉宽处理
    val commentsJsonDataStream: DataStream[String] = commentsWideBeanDataStream.map(commentsWideEntity => {
      JSON.toJSONString(commentsWideEntity, SerializerFeature.DisableCircularReferenceDetect)
    })

    commentsJsonDataStream.printToErr("拉宽后评论信息>>>")

    //4：将关联维度表后的数据写入到kafka中，供Druid进行指标分析
    commentsJsonDataStream.addSink(send2Kafka(GlobalConfigUtil.`output.topic.comments`))
  }
}