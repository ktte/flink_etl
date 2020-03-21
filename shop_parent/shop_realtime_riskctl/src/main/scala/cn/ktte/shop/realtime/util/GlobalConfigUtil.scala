package cn.ktte.shop.realtime.util

import com.typesafe.config.{Config, ConfigFactory}

/**
 * 全局的配置工具类.
 */
object GlobalConfigUtil {

  //加载application.conf配置文件
  private val config: Config = ConfigFactory.load()
  val `bootstrap.servers`: String = config.getString("bootstrap.servers")
  val `zookeeper.connect`: String = config.getString("zookeeper.connect")
  val `group.id`: String = config.getString("group.id")
  val `enable.auto.commit`: String = config.getString("enable.auto.commit")
  val `auto.commit.interval.ms`: String = config.getString("auto.commit.interval.ms")
  val `auto.offset.reset`: String = config.getString("auto.offset.reset")
  val `key.serializer`: String = config.getString("key.serializer")
  val `key.deserializer`: String = config.getString("key.deserializer")
  val `ip.file.path`: String = config.getString("ip.file.path")
  val `redis.server.ip`: String = config.getString("redis.server.ip")
  val `redis.server.port`: String = config.getString("redis.server.port")
  val `mysql.server.ip`: String = config.getString("mysql.server.ip")
  val `mysql.server.port`: String = config.getString("mysql.server.port")
  val `mysql.server.database`: String = config.getString("mysql.server.database")
  val `mysql.server.username`: String = config.getString("mysql.server.username")
  val `mysql.server.password`: String = config.getString("mysql.server.password")
  val `input.topic.canal`: String = config.getString("input.topic.canal")
  val `input.topic.click_log`: String = config.getString("input.topic.click_log")
  val `input.topic.cart`: String = config.getString("input.topic.cart")
  val `input.topic.comments`: String = config.getString("input.topic.comments")
  val `output.topic.order`: String = config.getString("output.topic.order")
  val `output.topic.order_detail`: String = config.getString("output.topic.order_detail")
  val `output.topic.cart`: String = config.getString("output.topic.cart")
  val `output.topic.clicklog`: String = config.getString("output.topic.clicklog")
  val `output.topic.goods`: String = config.getString("output.topic.goods")
  val `output.topic.ordertimeout`: String = config.getString("output.topic.ordertimeout")
  val `output.topic.comments`: String = config.getString("output.topic.comments")
  val `hbase.table.orderdetail`: String = config.getString("hbase.table.orderdetail")
  val `hbase.table.family`: String = config.getString("hbase.table.family")
}
