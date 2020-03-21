select
-- 大区的ID
t3.region_id,
-- 大区的名字
t3.region_name,
-- 一级分类ID
t2.cat_1t_id,
-- 一级分类名称
t2.cat_1t_name,
-- 支付方式ID
t4.payment_id,
-- 支付方式名称
t4.payment_name,
-- 订单数量 如果写成count(orderId)一定会出现重复的订单.
-- 因为订单明细宽表中存在重复的订单,所以一定要去重.
count(distinct order_id),
-- 订单金额
sum(pay_money)
from
(select * from itcast_dw.fact_order_goods_wide where dt = '20190909') t1
-- 关联商品分类表
left join
(select * from itcast_dw.dim_goods_cat where dt = '20190909') t2
on
t1.goods_cat_3d_id = t2.cat_3d_id
-- 关联大区
left join
(select * from itcast_dw.dim_shops where dt = '20190909') t3
on
t1.shop_id = t3.shop_id
-- 关联支付方式表
left join
(select * from itcast_dw.dim_payment where dt = '20190909') t4
on
t1.payment_id = t4.payment_id
group by
t2.cat_1t_id, t2.cat_1t_name, t3.region_id, t3.region_name, t4.payment_id, t4.payment_name;




    System.out.print(rowid);
    System.out.print(ogId);
    System.out.print(orderId);
    System.out.print(goodsId);
    System.out.print(goodsNum);
    System.out.print(goodsPrice);
    System.out.print(goodsName);
    System.out.print(shopId);
    System.out.print(goodsThirdCatId);
    System.out.print(goodsThirdCatName);
    System.out.print(goodsSecondCatId);
    System.out.print(goodsSecondCatName);
    System.out.print(goodsFirstCatId);
    System.out.print(goodsFirstCatName);
    System.out.print(areaId);
    System.out.print(shopName);
    System.out.print(shopCompany);
    System.out.print(cityId);
    System.out.print(cityName);
    System.out.print(regionId);
    System.out.print(regionName);