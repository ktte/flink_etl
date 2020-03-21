package cn.ktte.shop.realtime.etl.async

import cn.itcast.canal.bean.RowData
import cn.itcast.shop.realtime.etl.bean.{DimGoodsCatDBEntity, DimGoodsDBEntity, DimOrgDBEntity, DimShopsDBEntity, OrderGoodsWideEntity}
import cn.itcast.shop.realtime.etl.utils.RedisUtil
import cn.ktte.shop.realtime.etl.bean.{DimGoodsCatDBEntity, DimGoodsDBEntity, DimOrgDBEntity, DimShopsDBEntity, OrderGoodsWideEntity}
import cn.ktte.shop.realtime.etl.utils.RedisUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture, RichAsyncFunction}

/**
 * 订单明细拉宽操作,异步IO实现.
 */
class AsyncOrderGoodsDetailRedisRequest extends RichAsyncFunction[RowData, OrderGoodsWideEntity] {

  var jedis: Jedis = _

  override def open(parameters: Configuration): Unit = {
    // 获取Redis连接
    jedis = RedisUtil.getJedis()
  }


  /**
   * 如果数据超时,要做什么.
   *
   * @param input
   * @param resultFuture
   */
  override def timeout(input: RowData, resultFuture: ResultFuture[OrderGoodsWideEntity]): Unit = {
    println("超时了.")
  }

  //使用Future的时候,需要进行隐式参数导包

  // import scala.concurrent.ExecutionContext.Implicits._
  implicit lazy val executor = ExecutionContext.fromExecutor(Executors.directExecutor())

  //继承异步IO需要实现的方法.
  override def asyncInvoke(rowData: RowData, resultFuture: ResultFuture[OrderGoodsWideEntity]): Unit = {
    // 因为下方的所有操作都是异步的,需要用Future进行包裹.
    Future {
      // 利用Redis和RowData对数据进行拉宽.
      // 目标: 就是来构建OrderGoodsWideEntity
      //先获取能直接拿到的数据
      val ogId: String = rowData.getColumns.get("ogId")
      val orderId: String = rowData.getColumns.get("orderId")
      val goodsId: String = rowData.getColumns.get("goodsId")
      val goodsNum: String = rowData.getColumns.get("goodsNum")
      val goodsPrice: String = rowData.getColumns.get("goodsPrice")
      val goodsName: String = rowData.getColumns.get("goodsName")

      //获取商品的维度信息
      val goodsJson: String = jedis.hget("itcast_shop:dim_goods", goodsId)
      // 我们需要将商品的json字符串转换为对象,方便后续的操作.
      val goodsDBEntity: DimGoodsDBEntity = DimGoodsDBEntity(goodsJson)
      //获取商家ID和3级分类ID
      val shopId: Long = goodsDBEntity.getShopId
      val catId3: Int = goodsDBEntity.getGoodsCatId
      //获取分类的维度信息: 3级/2级/1级
      val cat3Entity: DimGoodsCatDBEntity = DimGoodsCatDBEntity(jedis.hget("itcast_shop:dim_goods_cats", catId3.toString))
      val catName3: String = cat3Entity.getCatName
      //获取2级分类信息
      val catId2: String = cat3Entity.getParentId
      val cat2Entity: DimGoodsCatDBEntity = DimGoodsCatDBEntity(jedis.hget("itcast_shop:dim_goods_cats", catId2.toString))
      val catName2: String = cat2Entity.getCatName
      //获取1级分类信息
      val catId1: String = cat2Entity.getParentId
      val cat1Entity: DimGoodsCatDBEntity = DimGoodsCatDBEntity(jedis.hget("itcast_shop:dim_goods_cats", catId1.toString))
      val catName1: String = cat1Entity.getCatName
      //获取商家的维度信息
      val shopsDBEntity: DimShopsDBEntity = DimShopsDBEntity(jedis.hget("itcast_shop:dim_shops", shopId.toString))
      val areaId: Int = shopsDBEntity.getAreaId
      val shopName: String = shopsDBEntity.getShopName
      val shopCompany: String = shopsDBEntity.getShopCompany
      //获取组织机构维度信息
      //获取城市信息
      val cityEntity: DimOrgDBEntity = DimOrgDBEntity(jedis.hget("itcast_shop:dim_org", areaId.toString))
      val cityId: Int = cityEntity.getOrgId
      val cityName: String = cityEntity.getOrgName
      //获取区域信息
      val regionId: Int = cityEntity.getParentId
      val regionEntity: DimOrgDBEntity = DimOrgDBEntity(jedis.hget("itcast_shop:dim_org", regionId.toString))
      val regionName: String = regionEntity.getOrgName

      //将上方构建的所有数据封装为订单明细宽表对象
      val wideEntity: OrderGoodsWideEntity = OrderGoodsWideEntity(
        ogId.toLong,
        orderId.toLong,
        goodsId.toLong,
        goodsNum.toLong,
        goodsPrice.toDouble,
        goodsName,
        shopId,
        catId3,
        catName3,
        catId2.toInt,
        catName2,
        catId1.toInt,
        catName1,
        areaId,
        shopName,
        shopCompany,
        cityId,
        cityName,
        regionId,
        regionName
      )

      // 我们数据处理完成之后需要告诉外界,我们执行完了,可以拿结果了
      resultFuture.complete(List(wideEntity))
    }

  }

}
