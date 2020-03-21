package cn.ktte.shop.realtime.etl.dataloader

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import cn.itcast.shop.realtime.etl.bean.{DimGoodsCatDBEntity, DimGoodsDBEntity, DimOrgDBEntity, DimShopCatDBEntity, DimShopsDBEntity}
import cn.itcast.shop.realtime.etl.utils.{GlobalConfigUtil, RedisUtil}
import cn.ktte.shop.realtime.etl.bean.{DimGoodsCatDBEntity, DimGoodsDBEntity, DimOrgDBEntity, DimShopCatDBEntity, DimShopsDBEntity}
import cn.ktte.shop.realtime.etl.utils.{GlobalConfigUtil, RedisUtil}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature

/**
 * 维度数据离线同步程序
 */
object DimensionDataLoader {









  def main(args: Array[String]): Unit = {
    //1. 先获取连接: MySQL连接/Redis连接
    //获取Redis连接
    val jedis: Jedis = RedisUtil.getJedis()

    // 注册驱动
    Class.forName("com.mysql.jdbc.Driver")
    //获取MySQL连接
    val connection: Connection = DriverManager.getConnection(
      s"jdbc:mysql://${GlobalConfigUtil.`mysql.server.ip`}:${GlobalConfigUtil.`mysql.server.port`}/${GlobalConfigUtil.`mysql.server.database`}",
      GlobalConfigUtil.`mysql.server.username`,
      GlobalConfigUtil.`mysql.server.password`
    )
    //创建statement
    val statement: Statement = connection.createStatement()
    // 加载商品维度数据
    loadDimGoodsData(jedis, statement)

    // 加载商品分类维度数据
    loadDimGoodsCatData(jedis, statement)

    // 加载商家维度数据
    loadDimShopsData(jedis, statement)

    // 加载组织机构维度数据
    loadDimOrgData(jedis, statement)

    // 加载商家商品分类维度数据()
    loadDimShopCatData(jedis, statement)

    //6. 关闭连接.
    jedis.close()
    statement.close()
    connection.close()
    //退出程序, 给个状态码标识,一般我们认为只有状态为0 的时候程序才是一个正常退出,否则就是有异常.
    System.exit(0)
  }

  /**
   * 加载商家商品分类维度数据
   * @param jedis
   * @param statement
   */
  def loadDimShopCatData(jedis: Jedis, statement: Statement) = {
    //2. 编写SQL查询维度数据(所有数据)
    val sql =
      """
        |SELECT
        |	catId,
        |	parentId,
        |	catName,
        |	catSort
        |FROM
        |	itcast_shop_cats
        |""".stripMargin
    //3. 将查询结果封装为实体类对象.
    val resultSet: ResultSet = statement.executeQuery(sql)
    while (resultSet.next()) {
      val catId = resultSet.getString("catId")
      val parentId = resultSet.getString("parentId")
      val catName = resultSet.getString("catName")
      val catSort = resultSet.getString("catSort")

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
  }


  /**
   * 加载组织机构维度数据
   * @param jedis
   * @param statement
   */
  def loadDimOrgData(jedis: Jedis, statement: Statement) = {
    //2. 编写SQL查询维度数据(所有数据)
    val sql =
      """
        |SELECT
        |	orgId,
        |	parentId,
        |	orgName,
        |	orgLevel
        |FROM
        |	itcast_org
        |""".stripMargin
    //3. 将查询结果封装为实体类对象.
    val resultSet: ResultSet = statement.executeQuery(sql)
    while (resultSet.next()) {
      val orgId = resultSet.getInt("orgId")
      val parentId = resultSet.getInt("parentId")
      val orgName = resultSet.getString("orgName")
      val orgLevel = resultSet.getInt("orgLevel")

      //将数据转换为对象
      val entity = DimOrgDBEntity(
        orgId,
        parentId,
        orgName,
        orgLevel)
      //4. 将实体对象转换为Json,便于存入Redis中.
      val json: String = JSON.toJSONString(entity, SerializerFeature.DisableCircularReferenceDetect)
      jedis.hset("itcast_shop:dim_org", orgId.toString, json)

    }
  }



  /**
   * 加载商家维度数据
   * @param jedis
   * @param statement
   */
  def loadDimShopsData(jedis: Jedis, statement: Statement) = {
    //2. 编写SQL查询维度数据(所有数据)
    val sql =
      """
        |SELECT
        |	shopId,
        |	areaId,
        |	shopName,
        |	shopCompany
        |FROM
        |	itcast_shops
        |""".stripMargin
    //3. 将查询结果封装为实体类对象.
    val resultSet: ResultSet = statement.executeQuery(sql)
    while (resultSet.next()) {
      val shopId = resultSet.getInt("shopId")
      val areaId = resultSet.getInt("areaId")
      val shopName = resultSet.getString("shopName")
      val shopCompany = resultSet.getString("shopCompany")

      //将数据转换为对象
      val entity = DimShopsDBEntity(
        shopId,
        areaId,
        shopName,
        shopCompany)
      //4. 将实体对象转换为Json,便于存入Redis中.
      val json: String = JSON.toJSONString(entity, SerializerFeature.DisableCircularReferenceDetect)
      jedis.hset("itcast_shop:dim_shops", shopId.toString, json)

    }
  }

  /**
   * 加载商品分类维度数据
   * @param jedis
   * @param statement
   */
  def loadDimGoodsCatData(jedis: Jedis, statement: Statement) = {
    //2. 编写SQL查询维度数据(所有数据)
    val sql =
      """
        |SELECT
        |	catId,
        |	parentId,
        |	catName,
        |	cat_level
        |FROM
        |	itcast_goods_cats
        |""".stripMargin
    //3. 将查询结果封装为实体类对象.
    val resultSet: ResultSet = statement.executeQuery(sql)
    while (resultSet.next()) {
      val catId = resultSet.getString("catId")
      val parentId = resultSet.getString("parentId")
      val catName = resultSet.getString("catName")
      val cat_level = resultSet.getString("cat_level")

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
  }

  /**
   * 加载商品维度数据
   * @param jedis
   * @param statement
   */
  private def loadDimGoodsData(jedis: Jedis, statement: Statement): Unit = {
    //2. 编写SQL查询维度数据(所有数据)
    val sql =
      """
        |SELECT
        |	goodsId,
        |	goodsName,
        |	shopId,
        |	goodsCatId,
        |	shopPrice
        |FROM
        |	itcast_goods
        |""".stripMargin
    //3. 将查询结果封装为实体类对象.
    val resultSet: ResultSet = statement.executeQuery(sql)
    while (resultSet.next()) {
      val goodsId = resultSet.getLong("goodsId")
      val goodsName = resultSet.getString("goodsName")
      val shopId = resultSet.getLong("shopId")
      val goodsCatId = resultSet.getInt("goodsCatId")
      val shopPrice = resultSet.getDouble("shopPrice")

      //将数据转换为对象
      val entity: DimGoodsDBEntity = DimGoodsDBEntity(goodsId, goodsName, shopId, goodsCatId, shopPrice)
      //4. 将实体对象转换为Json,便于存入Redis中.
      val json: String = JSON.toJSONString(entity, SerializerFeature.DisableCircularReferenceDetect)
      //5. 将数据存入redis.
      //   Redis是Key-Value型的数据库.   hello=>world
      //   比如: key: itcast_goods    value(hash): (goodsId, json)
      //如果直接用商品ID作为key,最终Redis中的Key会变得很多.不方便管理
      //我们可以将数据存储设计为map结构(goodsID, Json)
      jedis.hset("itcast_shop:dim_goods", goodsId.toString, json)

    }
  }
}
