package cn.ktte.canal.protobuf;

/**
 * 凡是需要通过protobuf实现序列化,向Kafka中发送数据的自定义对象都需要实现此接口.
 * 比如现在我想将User这个对象使用protobuf向Kafka中发送.那么只需要让User实现ProtoBufable这个接口即可
 */
public interface ProtoBufable {

    /**
     * 子类实现,必须要重写的方法,儿子自己决定如何来生成这个byte数组.
     * @return
     */
    byte[] toByte();

}
