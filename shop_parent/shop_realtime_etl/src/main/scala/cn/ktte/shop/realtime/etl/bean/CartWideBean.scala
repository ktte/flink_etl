package cn.ktte.shop.realtime.etl.bean

import com.alibaba.fastjson.JSON
import org.apache.commons.lang.time.DateFormatUtils

import scala.beans.BeanProperty

/**
 * 购物车样例类
 */
case class CartBean(goodsId: String,
                    userId: String,
                    count: Integer,
                    guid: String,
                    addTime: String,
                    ip: String);

object CartBean {
  def apply(json: String): CartBean = {
    val jsonObject = JSON.parseObject(json)
    CartBean(
      jsonObject.getString("goodsId"),
      jsonObject.getString("userId"),
      jsonObject.getInteger("count"),
      jsonObject.getString("guid"),
      jsonObject.getString("addTime"),
      jsonObject.getString("ip"))
  }
}



case class CartWideBean(
                         @BeanProperty goodsId: String, //商品id
                         @BeanProperty userId: String, //用户id
                         @BeanProperty count: Integer, //商品数量
                         @BeanProperty guid: String, //用户唯一标识
                         @BeanProperty addTime: String, //添加购物车时间
                         @BeanProperty ip: String, //ip地址
                         @BeanProperty var goodsPrice:Double, //商品价格
                         @BeanProperty var goodsName:String, //商品名称
                         @BeanProperty var goodsCat3:String, //商品三级分类
                         @BeanProperty var goodsCat2:String, //商品二级分类
                         @BeanProperty var goodsCat1:String, //商品一级分类
                         @BeanProperty var shopId:String, //门店id
                         @BeanProperty var shopName:String, //门店名称
                         @BeanProperty var shopProvinceId:String, //门店所在省份id
                         @BeanProperty var shopProvinceName:String, //门店所在省份名称
                         @BeanProperty var shopCityId:String, //门店所在城市id
                         @BeanProperty var shopCityName:String, //门店所在城市名称
                         @BeanProperty var clientProvince:String, //客户所在省份
                         @BeanProperty var clientCity:String //客户所在城市
                       );

object CartWideBean {
  def apply(cartBean: CartBean): CartWideBean = {
    CartWideBean(cartBean.goodsId,
      cartBean.userId,
      cartBean.count,
      cartBean.guid,
      DateFormatUtils.format(cartBean.addTime.toLong, "yyyy-MM-dd HH:mm:ss"),
      cartBean.ip,
      0, "", "", "", "", "", "", "", "", "", "", "", "")
  }
}