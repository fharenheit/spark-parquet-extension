package io.datadynamics.bigdata.spark.extension;

import redis.clients.jedis.Jedis;

public class RedisUtils {

    public static void send(String server, String channel, String message) {
        Jedis jedis = new Jedis(server);
        jedis.publish(channel, message);
        System.out.println(message);
    }

}
