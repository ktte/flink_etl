package cn.ktte.shop.realtime.etl.utils

import cn.itcast.canal.bean.RowData
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema


/**
 * 自定义的RowData转换类,将Kafka中的数据直接转换为RowData
 */
class CanalRowDataDeserializationSchema extends AbstractDeserializationSchema[RowData]{
  //在此方法中,将字节数据转换为RowData.
  override def deserialize(bytes: Array[Byte]): RowData = {
    // 1. 要么在这个方法中进行转换(不建议.)
    // 2. 要么在RowData中提供构造方法,在RowData中进行转换(可以方便后期别的地方来使用)
    new RowData(bytes)
  }
}
