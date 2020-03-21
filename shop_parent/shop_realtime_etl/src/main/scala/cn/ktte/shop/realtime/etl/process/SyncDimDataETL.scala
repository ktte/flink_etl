package cn.ktte.shop.realtime.etl.process

import cn.itcast.canal.bean.RowData
import cn.itcast.shop.realtime.etl.bean.{DimGoodsCatDBEntity, DimGoodsDBEntity, DimOrgDBEntity, DimShopCatDBEntity, DimShopsDBEntity}
import cn.itcast.shop.realtime.etl.utils.RedisUtil
import cn.ktte.shop.realtime.etl.bean.{DimGoodsCatDBEntity, DimGoodsDBEntity, DimOrgDBEntity, DimShopCatDBEntity, DimShopsDBEntity}
import cn.ktte.shop.realtime.etl.process.base.MySQLBaseETL
import cn.ktte.shop.realtime.etl.utils.RedisUtil
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._

/**
 * 同步离线数据ETL程序
 */
class SyncDimDataETL(env: StreamExecutionEnvironment) extends MySQLBaseETL(env) {
  override def process(): Unit = {
    //1. 过滤: 只要维度相关的表数据,其它都不要.
    getDataSource()
      .filter(rowData => {
        //判断当前的表是否是我们需要的维度表
        //因为表比较多,我们可以用模式匹配进行操作
        rowData.getTableName match {
          case "itcast_goods" => true
          case "itcast_goods_cats" => true
          case "itcast_shops" => true
          case "itcast_org" => true
          case "itcast_shop_cats" => true
          //全部都没有匹配上,用_表示
          case _ => false
        }
      })
      //2. 数据落地:
      .addSink(new RichSinkFunction[RowData] {
        var jedis: Jedis = _

        override def open(parameters: Configuration): Unit = {
          //在open方法中获取Redis的连接
          jedis = RedisUtil.getJedis()
        }


        override def invoke(rowData: RowData): Unit = {
          //   判断当前操作的类型.
          //          if (rowData.getEventType.equalsIgnoreCase("delete")){
          //          } else {}
          //
          //用模式匹配找到要执行的操作
          rowData.getEventType.toLowerCase match {
            //   如果是删除,那么只需删除,
            case "delete" => deleteData(rowData)
            //   否则插入数据.
            case "insert" => insertData(rowData)
            case "update" => insertData(rowData)
            case _ => // 如果都没匹配上,那么这个invoke什么都不做.
          }
        }

        /**
         * 新增数据
         *
         * @param rowData
         */
        def insertData(rowData: RowData): Unit = {
          println(s"修改维度数据: ${rowData.getTableName}, 数据: ${rowData.getColumns}")

          //1. 先判断当前记录是哪张表
          rowData.getTableName match {
            //如果是商品表
            case "itcast_goods" => {
              //2. 比如当前是一个商品表
              //3. 获取当前RowData中的数据,将数据存入Redis.
              val goodsId = rowData.getColumns.get("goodsId")
              val goodsName = rowData.getColumns.get("goodsName")
              val shopId = rowData.getColumns.get("shopId")
              val goodsCatId = rowData.getColumns.get("goodsCatId")
              val shopPrice = rowData.getColumns.get("shopPrice")
              //将数据封装为DimGoodsDBEntity对象
              val entity: DimGoodsDBEntity = DimGoodsDBEntity(goodsId.toLong, goodsName, shopId.toLong, goodsCatId.toInt, shopPrice.toDouble)
              //将对象转换为json
              val json: String = JSON.toJSONString(entity, SerializerFeature.DisableCircularReferenceDetect)
              //存入Redis
              jedis.hset("itcast_shop:dim_goods", goodsId.toString, json)
            }
            case "itcast_goods_cats" => {
              val catId = rowData.getColumns.get("catId")
              val parentId = rowData.getColumns.get("parentId")
              val catName = rowData.getColumns.get("catName")
              val cat_level = rowData.getColumns.get("cat_level")
              //将数据转换为对象
              val entity = DimGoodsCatDBEntity(
                catId,
                parentId,
                catName,
                cat_level)
              //4. 将实体对象转换为Json,便于存入Redis中.
              val json: String = JSON.toJSONString(entity, SerializerFeature.DisableCircularReferenceDetect)
              jedis.hset("itcast_shop:dim_goods_cats", catId.toString, json)
            }
            case "itcast_shops" => {
              val shopId = rowData.getColumns.get("shopId")
              val areaId = rowData.getColumns.get("areaId")
              val shopName = rowData.getColumns.get("shopName")
              val shopCompany = rowData.getColumns.get("shopCompany")

              //将数据转换为对象
              val entity = DimShopsDBEntity(
                shopId.toInt,
                areaId.toInt,
                shopName,
                shopCompany)
              //4. 将实体对象转换为Json,便于存入Redis中.
              val json: String = JSON.toJSONString(entity, SerializerFeature.DisableCircularReferenceDetect)
              jedis.hset("itcast_shop:dim_shops", shopId.toString, json)

            }
            case "itcast_org" => {
              val orgId = rowData.getColumns.get("orgId")
              val parentId = rowData.getColumns.get("parentId")
              val orgName = rowData.getColumns.get("orgName")
              val orgLevel = rowData.getColumns.get("orgLevel")

              //将数据转换为对象
              val entity = DimOrgDBEntity(
                orgId.toInt,
                parentId.toInt,
                orgName,
                orgLevel.toInt)
              //4. 将实体对象转换为Json,便于存入Redis中.
              val json: String = JSON.toJSONString(entity, SerializerFeature.DisableCircularReferenceDetect)
              jedis.hset("itcast_shop:dim_org", orgId.toString, json)
            }
            case "itcast_shop_cats" => {
              val catId = rowData.getColumns.get("catId")
              val parentId = rowData.getColumns.get("parentId")
              val catName = rowData.getColumns.get("catName")
              val catSort = rowData.getColumns.get("catSort")
              //将数据转换为对象
              val entity = DimShopCatDBEntity(
                catId,
                parentId,
                catName,
                catSort)
              //4. 将实体对象转换为Json,便于存入Redis中.
              val json: String = JSON.toJSONString(entity, SerializerFeature.DisableCircularReferenceDetect)
              jedis.hset("itcast_shop:dim_shop_cats", catId.toString, json)

            }
            case _ =>
          }


        }

        /**
         * 删除Redis 中的数据
         *
         * @param rowData
         */
        def deleteData(rowData: RowData): Unit = {

          //删除Redis中的维度数据
          println(s"删除数据: ${rowData.getTableName}, 数据: ${rowData.getColumns}")

          //我们的数据是以hash的格式进行保存的.所以,删除数据还是得用的hash的api
          //          jedis.hdel(key,fields)
          rowData.getTableName match {
            //如果是商品表
            case "itcast_goods" => jedis.hdel("itcast_shop:dim_goods", rowData.getColumns.get("goodsId"))
            case "itcast_goods_cats" => jedis.hdel("itcast_shop:dim_goods_cats", rowData.getColumns.get("catId"))
            case "itcast_shops" => jedis.hdel("itcast_shop:dim_shops", rowData.getColumns.get("shopId"))
            case "itcast_org" => jedis.hdel("itcast_shop:dim_org", rowData.getColumns.get("orgId"))
            case "itcast_shop_cats" => jedis.hdel("itcast_shop:dim_shop_cats", rowData.getColumns.get("catId"))
            case _ =>
          }
          jedis.set("hello", "world")
          jedis.del("hello")
          /*

          User {
            username
            age
            birthday

          }

          key: user    value(username: zhangsan, birthday: 1991)
          key: itcast_shop:dim_goods value(100102: {}, 100103: {})
          del (user, age)






          */


        }


        override def close(): Unit = {
          if (jedis != null) {
            jedis.close()
          }
        }
      })


  }
}
