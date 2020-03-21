package cn.ktte.canal.bean;

import cn.ktte.canal.protobuf.ProtoBufable;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * 向Kafka发送数据的实体类,需要实现ProtoBufable才能向Kafka中发送.
 */
// 如果使用lombok,一定要确保idea有lombok插件.
@Data //getSet方法.
@NoArgsConstructor // 无参构造
public class RowData implements ProtoBufable {

    private String logfilename;
    private Long logfileoffset;
    private Long executeTime;
    private String schemaName;
    private String tableName;
    private String eventType;
    private Map<String, String> columns;

    /**
     * 将传递进来的Map转换为RowData对象
     * @param binlogMsgMap
     */
    public RowData(Map binlogMsgMap) {
        logfilename = binlogMsgMap.get("logfileName").toString();
        logfileoffset = Long.parseLong(binlogMsgMap.get("logfileOffset").toString());
        executeTime = Long.parseLong(binlogMsgMap.get("executeTime").toString());
        schemaName = binlogMsgMap.get("schemaName").toString();
        tableName = binlogMsgMap.get("tableName").toString();
        eventType = binlogMsgMap.get("eventType").toString();
        columns = (Map<String, String>) binlogMsgMap.get("columns");
    }

    /**
     * 接收字节数据,创建RowData对象.
     * 这个字节数据是从Kafka中获取到的
     * @param bytes
     */
    public RowData(byte[] bytes) {
        try {
            //1. 解析字节数据,获取到CanalModel.RowData对象
            CanalModel.RowData rowData = CanalModel.RowData.parseFrom(bytes);
            //2. 获取CanalModel.RowData中的数据,并赋值给当前RowData对象.
            this.logfilename = rowData.getLogfilename();
            this.logfileoffset = rowData.getLogfileoffset();
            this.executeTime = rowData.getExecuteTime();
            this.schemaName = rowData.getSchemaName();
            this.tableName = rowData.getTableName();
            this.eventType = rowData.getEventType();
            //先对columns进行初始化
            this.columns = new HashMap<>();
            //将rowdata中的列信息复制到当前的columns
            columns.putAll(rowData.getColumnsMap());

        } catch (InvalidProtocolBufferException e) {
            //将异常信息在控制台进行打印,实际开发中,一般都会在此处用log4j将日志信息存入文件中.
            e.printStackTrace();
        }
    }


    /**
     * 这个方法,是我们实现将自定义的对象转成字节的核心方法,
     * 通过此方法,我们将当前类中的成员变量转换为Protobuf字节数组并返回.
     * @return
     */
    @Override
    public byte[] toByte() {
        // 1. 当前类中需要有变量
        // 2. 定义protobuf.
        //将上面的成员变量封装为Protobuf
        CanalModel.RowData.Builder builder = CanalModel.RowData.newBuilder();
        builder.setLogfilename(logfilename);
        builder.setLogfileoffset(logfileoffset);
        builder.setExecuteTime(executeTime);
        builder.setSchemaName(schemaName);
        builder.setTableName(tableName);
        builder.setEventType(eventType);
        //想build中添加map数据
        builder.putAllColumns(columns);
        //将数据转换为字节数组
        byte[] bytes = builder.build().toByteArray();
        return bytes;
    }
}
