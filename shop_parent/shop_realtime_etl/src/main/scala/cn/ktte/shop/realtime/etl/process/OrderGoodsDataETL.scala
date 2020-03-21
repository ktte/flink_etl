package cn.ktte.shop.realtime.etl.process

import java.util.concurrent.TimeUnit

import cn.itcast.canal.bean.RowData
import cn.itcast.shop.realtime.etl.async.AsyncOrderGoodsDetailRedisRequest
import cn.itcast.shop.realtime.etl.bean.OrderGoodsWideEntity
import cn.itcast.shop.realtime.etl.utils.{GlobalConfigUtil, HbaseUtil}
import cn.ktte.shop.realtime.etl.async.AsyncOrderGoodsDetailRedisRequest
import cn.ktte.shop.realtime.etl.bean.OrderGoodsWideEntity
import cn.ktte.shop.realtime.etl.process.base.MySQLBaseETL
import cn.ktte.shop.realtime.etl.utils.{GlobalConfigUtil, HbaseUtil}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.{AsyncFunction, ResultFuture}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put, Table}

/**
 * 订单明细实时拉宽业务处理ETL程序
 *
 * @param env
 */
class OrderGoodsDataETL(env: StreamExecutionEnvironment) extends MySQLBaseETL(env) {
  override def process(): Unit = {
    val dataSource: DataStream[RowData] = getDataSource()
      //    1. 首先做一个过滤:只要订单明细的数据:`itcast_order_goods`,因为订单明细是不会发生改变的,所以我们只需要`insert`事件即可
      .filter(_.getTableName == "itcast_order_goods")
      .filter(_.getEventType == "insert")
    //2. 因为现在获取到的数据是RowData类型.我们可以直接从RowData中获取订单ID,商品ID等信息.那么我们可以直接使用这些数据对订单明细进行拉宽
    //3. 如何拉宽? 比如来一个map操作.在Map中获取Redis中的数据,然后将数据组装成宽表数据.(异步IO)
    //map操作是一个同步的方法.
    //同步: 排队,一个一个上,处理完前面的,再处理后面的.运行时间等于最后一条任务完成的时间.
    //异步: 给个小票,等着取结果就可以了.这样可以提高整体的运行效率.运行时间等于耗时最长的任务.

    //使用异步IO来解决数据拉宽操作
    // 参数1: 要用异步IO进行处理的数据源
    // 参数2: 要进行处理的具体内容.类似于map操作.
    // 参数3: 超时时间,等待结果最多需要多久
    // 参数4: 时间单位
    // 参数5: 异步IO的并行度多少.
    val asyncDataStream: DataStream[OrderGoodsWideEntity] = AsyncDataStream.unorderedWait(
      dataSource,
      new AsyncOrderGoodsDetailRedisRequest,
      1000L,
      TimeUnit.SECONDS, 100)
    // asyncDataStream就是异步IO处理之后的数据
    //    asyncDataStream.print()

    //4. 将最终拉宽后的数据发送Kafka
    // 将数据转换为json
    asyncDataStream.map(JSON.toJSONString(_, SerializerFeature.DisableCircularReferenceDetect))
      //发送Kafka
      .addSink(send2Kafka(GlobalConfigUtil.`output.topic.order_detail`))

    //打印数据
    asyncDataStream.print("订单明细:")

    //5. 将结果在HBase中也保留一份.
    asyncDataStream.addSink(new RichSinkFunction[OrderGoodsWideEntity] {
      var table: Table = _

      override def open(parameters: Configuration): Unit = {
        //获取Hbase的连接
        val connection: Connection = HbaseUtil.getPool().getConnection
        // 提前在Hbase中创建表. create "dwd_order_detail","detail"
        table = connection.getTable(TableName.valueOf(GlobalConfigUtil.`hbase.table.orderdetail`))
      }

      override def invoke(wideEntity: OrderGoodsWideEntity): Unit = {
        //将wideEntity 中的数据保存到Hbase就可以了.
        //        table.put(new Put())
        //        table.delete()
        //        table.get()
        val put = new Put(wideEntity.getOgId.toString.getBytes())
        //向put中添加列信息
        val familyName: Array[Byte] = GlobalConfigUtil.`hbase.table.family`.getBytes()
        put.addColumn(familyName, "ogId".getBytes, wideEntity.ogId.toString.getBytes)
        put.addColumn(familyName, "orderId".getBytes, wideEntity.orderId.toString.getBytes)
        put.addColumn(familyName, "goodsId".getBytes, wideEntity.goodsId.toString.getBytes)
        put.addColumn(familyName, "goodsNum".getBytes, wideEntity.goodsNum.toString.getBytes)
        put.addColumn(familyName, "goodsPrice".getBytes, wideEntity.goodsPrice.toString.getBytes)
        put.addColumn(familyName, "goodsName".getBytes, wideEntity.goodsName.toString.getBytes)
        put.addColumn(familyName, "shopId".getBytes, wideEntity.shopId.toString.getBytes)
        put.addColumn(familyName, "goodsThirdCatId".getBytes, wideEntity.goodsThirdCatId.toString.getBytes)
        put.addColumn(familyName, "goodsThirdCatName".getBytes, wideEntity.goodsThirdCatName.toString.getBytes)
        put.addColumn(familyName, "goodsSecondCatId".getBytes, wideEntity.goodsSecondCatId.toString.getBytes)
        put.addColumn(familyName, "goodsSecondCatName".getBytes, wideEntity.goodsSecondCatName.toString.getBytes)
        put.addColumn(familyName, "goodsFirstCatId".getBytes, wideEntity.goodsFirstCatId.toString.getBytes)
        put.addColumn(familyName, "goodsFirstCatName".getBytes, wideEntity.goodsFirstCatName.toString.getBytes)
        put.addColumn(familyName, "areaId".getBytes, wideEntity.areaId.toString.getBytes)
        put.addColumn(familyName, "shopName".getBytes, wideEntity.shopName.toString.getBytes)
        put.addColumn(familyName, "shopCompany".getBytes, wideEntity.shopCompany.toString.getBytes)
        put.addColumn(familyName, "cityId".getBytes, wideEntity.cityId.toString.getBytes)
        put.addColumn(familyName, "cityName".getBytes, wideEntity.cityName.toString.getBytes)
        put.addColumn(familyName, "regionId".getBytes, wideEntity.regionId.toString.getBytes)
        put.addColumn(familyName, "regionName".getBytes, wideEntity.regionName.toString.getBytes)
        table.put(put)
      }
    })

  }
}
