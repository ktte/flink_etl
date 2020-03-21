package cn.ktte.log

import java.io.File

import cn.ktte.shop.realtime.etl.bean.ip.IPSeeker

object IpTest {

  def main(args: Array[String]): Unit = {
    val seeker = new IPSeeker(new File("C:\\My_Data\\IDEA_WorkSpace\\itcast_shop_parent_17\\data\\qqwry.dat"))
    val area: String = seeker.getArea("222.68.172.190")
    val country: String = seeker.getCountry("222.68.172.190")
    println(area)
    println(country)
  }

}
