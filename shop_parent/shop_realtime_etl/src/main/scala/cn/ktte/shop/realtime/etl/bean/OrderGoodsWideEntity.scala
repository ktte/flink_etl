package cn.ktte.shop.realtime.etl.bean

import scala.beans.BeanProperty

// 订单明细拉宽数据
case class OrderGoodsWideEntity(@BeanProperty ogId:Long, // 订单明细ID
                                @BeanProperty orderId:Long, // 订单ID
                                @BeanProperty goodsId:Long, // 商品ID
                                @BeanProperty goodsNum:Long, // 商品数量
                                @BeanProperty goodsPrice:Double, // 商品价格
                                @BeanProperty goodsName:String, // 商品名称
                                @BeanProperty shopId:Long, // 商家ID
                                @BeanProperty goodsThirdCatId:Int, // 3级分类ID
                                @BeanProperty goodsThirdCatName:String, // 3级分类名称
                                @BeanProperty goodsSecondCatId:Int, // 2级分类ID
                                @BeanProperty goodsSecondCatName:String, // 2级分类名称
                                @BeanProperty goodsFirstCatId:Int, // 1级分类ID
                                @BeanProperty goodsFirstCatName:String, // 1级分类名称
                                @BeanProperty areaId:Int, // 区域ID
                                @BeanProperty shopName:String, // 商家名称
                                @BeanProperty shopCompany:String, // 商家所属公司名称
                                @BeanProperty cityId:Int, // 城市ID
                                @BeanProperty cityName:String, // 城市名称
                                @BeanProperty regionId:Int, // 区域ID
                                @BeanProperty regionName:String // 区域名称
                               )