package cn.ktte.log;

import redis.clients.jedis.Jedis;

/**
 * Redis测试
 */
public class JedisTest {
    public static void main(String[] args) {
        // 先获取Redis操作对象
        Jedis jedis = new Jedis("node2", 6379);
        jedis.select(1);
        jedis.set("hello", "world");
//        String json = jedis.hget("itcast_shop:dim_goods", "112777");
//        System.out.println(json);


    }
}
