package cn.ktte.shop.realtime.etl.bean

import com.alibaba.fastjson.JSON

import scala.beans.BeanProperty

/**
 * 维度类:
 * 包含商品维度样例类/商家/组织机构....
 */
// 商品维度样例类
case class DimGoodsDBEntity(@BeanProperty goodsId:Long,		// 商品id
                            @BeanProperty goodsName:String,	// 商品名称
                            @BeanProperty var shopId:Long,			// 店铺id
                            @BeanProperty goodsCatId:Int,   // 商品分类id
                            @BeanProperty shopPrice:Double) // 商品价格
object DimGoodsDBEntity{
  //定义一个apply方法,接收string类型json,转换为对象
  def apply(json: String): DimGoodsDBEntity = {
    // 将json => DimGoodsDBEntity
    JSON.parseObject(json, classOf[DimGoodsDBEntity])
  }
}
// 商品分类维度样例类
case class DimGoodsCatDBEntity(@BeanProperty catId:String,	// 商品分类id
                               @BeanProperty parentId:String,	// 商品分类父id
                               @BeanProperty catName:String,	// 商品分类名称
                               @BeanProperty cat_level:String)	// 商品分类级别
object DimGoodsCatDBEntity{
  //定义一个apply方法,接收string类型json,转换为对象
  def apply(json: String): DimGoodsCatDBEntity = {
    // 将json => DimGoodsDBEntity
    JSON.parseObject(json, classOf[DimGoodsCatDBEntity])
  }
}
// 店铺维度样例类
case class DimShopsDBEntity(@BeanProperty shopId:Int,		// 店铺id
                            @BeanProperty areaId:Int,		// 店铺所属区域id
                            @BeanProperty shopName:String,	// 店铺名称
                            @BeanProperty shopCompany:String)	// 公司名称
object DimShopsDBEntity{
  //定义一个apply方法,接收string类型json,转换为对象
  def apply(json: String): DimShopsDBEntity = {
    // 将json => DimGoodsDBEntity
    JSON.parseObject(json, classOf[DimShopsDBEntity])
  }
}
// 组织结构维度样例类
case class DimOrgDBEntity(@BeanProperty orgId:Int,			// 机构id
                          @BeanProperty parentId:Int,		// 机构父id
                          @BeanProperty orgName:String,		// 组织机构名称
                          @BeanProperty orgLevel:Int)		// 组织机构级别
object DimOrgDBEntity{
  //定义一个apply方法,接收string类型json,转换为对象
  def apply(json: String): DimOrgDBEntity = {
    // 将json => DimGoodsDBEntity
    JSON.parseObject(json, classOf[DimOrgDBEntity])
  }
}
// 门店商品分类维度样例类
case class DimShopCatDBEntity(@BeanProperty catId:String = "",	 // 商品分类id
                              @BeanProperty parentId:String = "",// 商品分类父id
                              @BeanProperty catName:String = "", // 商品分类名称
                              @BeanProperty catSort:String = "")// 商品分类级别
object DimShopCatDBEntity{
  //定义一个apply方法,接收string类型json,转换为对象
  def apply(json: String): DimShopCatDBEntity = {
    // 将json => DimGoodsDBEntity
    JSON.parseObject(json, classOf[DimShopCatDBEntity])
  }
}