package cn.ktte.shop.realtime.etl.app

import java.util.Properties

import cn.itcast.canal.bean.RowData
import cn.itcast.shop.realtime.etl.process.{CartETL, ClickLogETL, GoodsETL, OrderETL, OrderGoodsDataETL, SyncDimDataETL}
import cn.itcast.shop.realtime.etl.utils.{CanalRowDataDeserializationSchema, GlobalConfigUtil}
import cn.ktte.shop.realtime.etl.process.{CartETL, ClickLogETL, GoodsETL, OrderETL, OrderGoodsDataETL, SyncDimDataETL}
import cn.ktte.shop.realtime.etl.utils.GlobalConfigUtil
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
 * 实时数仓ETL主程序
 */
object App {

  def main(args: Array[String]): Unit = {
    //    1. 获取Flink流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2. 设置并行度
    env.setParallelism(1)
    //3. 设置时间的处理策略
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //4. 开启Checkpoint/设置Checkpoint
    env.enableCheckpointing(5000L)
    //设置Checkpoint相关参数
    // 设置模式为仅一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置超时时间,一般都会设置稍微大一点,主要是防止慢磁盘.
    env.getCheckpointConfig.setCheckpointTimeout(60000L)
    //设置2次checkpoint之间的最小时间间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000L)
    // 设置checkpoint的最大的并行度.
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 如果程序是正常退出,我们也要保留最后一次checkpoint的数据.
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //将数据保存起来,Flink的数据保存策略: 保存到内存 / 保存到存储系统(本地/HDFS) / RocketsDB + 存储系统.
    env.setStateBackend(new FsStateBackend("hdfs://node1:8020/itcast_shop_chk0000"))

    //5. 设置重启策略. 如果程序出错,一秒钟后重启.重启1次.
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 1000))

    //将纯真IP地址库文件放入分布式缓存中
    env.registerCachedFile(GlobalConfigUtil.`ip.file.path`,"qqwry.dat")

    //7. 开始业务处理
    // 处理订单数据
    new OrderETL(env).process()
    // 处理点击流日志数据
    new ClickLogETL(env).process()
    // 同步维度数据到Redis中
    new SyncDimDataETL(env).process()
    //处理订单明细数据,实时拉宽ETL
    new OrderGoodsDataETL(env).process()
    // 处理商品数据,实时拉宽ETL开发
    new GoodsETL(env).process()
    // 处理购物车数据,实时拉宽ETL开发
    new CartETL(env).process()





    //8. 启动程序.
    env.execute()
  }
}
