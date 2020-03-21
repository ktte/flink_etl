package com.ktte.canal_client.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 读取配置文件的工具类
 */
public class ConfigUtil {

    public static String CANAL_SERVER_IP = "";
    public static Integer CANAL_SERVER_PORT = 0;
    public static String CANAL_SERVER_DESTINATION = "";
    public static String CANAL_SERVER_USERNAME = "";
    public static String CANAL_SERVER_PASSWORD = "";
    public static String CANAL_SUBSCRIBE_FILTER = "";
    public static String ZOOKEEPER_SERVER_IP = "";
    public static String KAFKA_BOOTSTRAP_SERVERS_CONFIG = "";
    public static Integer KAFKA_BATCH_SIZE_CONFIG = 0;
    public static String KAFKA_ACKS = "";
    public static Integer KAFKA_RETRIES = 0;
    public static String KAFKA_KEY_SERIALIZER_CLASS_CONFIG = "";
    public static String KAFKA_VALUE_SERIALIZER_CLASS_CONFIG = "";
    public static String KAFKA_TOPIC = "";

    //我们可以将赋值的操作放入代码块中.
    //普通代码块: 随着对象的创建而加载
    //静态代码块: 随着类的加载而加载.
    static {
        try {
            // 获取configproperties配置文件对象
            // 1. 定义properties对象
            Properties properties = new Properties();
            // 2. 加载config.properties文件内容到properties对象中
//        new File("config.properties");
            //用户画像,类的加载器.
            //使用类的加载器来获取数据.
            InputStream inputStream = ConfigUtil.class.getClassLoader().getResourceAsStream("config.properties");
            properties.load(inputStream);
            // 3. 开始赋值
            CANAL_SERVER_IP = properties.getProperty("canal.server.ip");
            CANAL_SERVER_PORT = Integer.parseInt(properties.getProperty("canal.server.port"));
            CANAL_SERVER_DESTINATION = properties.getProperty("canal.server.destination");
            CANAL_SERVER_USERNAME = properties.getProperty("canal.server.username");
            CANAL_SERVER_PASSWORD = properties.getProperty("canal.server.password");
            CANAL_SUBSCRIBE_FILTER = properties.getProperty("canal.subscribe.filter");
            ZOOKEEPER_SERVER_IP = properties.getProperty("zookeeper.server.ip");
            KAFKA_BOOTSTRAP_SERVERS_CONFIG = properties.getProperty("kafka.bootstrap_servers_config");
            KAFKA_BATCH_SIZE_CONFIG = Integer.parseInt(properties.getProperty("kafka.batch_size_config"));
            KAFKA_ACKS = properties.getProperty("kafka.acks");
            KAFKA_RETRIES = Integer.parseInt(properties.getProperty("kafka.retries"));
            KAFKA_KEY_SERIALIZER_CLASS_CONFIG = properties.getProperty("kafka.key_serializer_class_config");
            KAFKA_VALUE_SERIALIZER_CLASS_CONFIG = properties.getProperty("kafka.value_serializer_class_config");
            KAFKA_TOPIC = properties.getProperty("kafka.topic");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        }
    }

    public static void main(String[] args) {
        System.out.println(ConfigUtil.CANAL_SERVER_PASSWORD);
        System.out.println(ConfigUtil.KAFKA_BATCH_SIZE_CONFIG);
        System.out.println(ConfigUtil.KAFKA_BOOTSTRAP_SERVERS_CONFIG);
        System.out.println(ConfigUtil.KAFKA_TOPIC);
    }



}
