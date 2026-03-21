package com.music.util;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisUtil {

    private static final String SONG_DIM_KEY_PREFIX = "dim:song:";

    private RedisUtil() {
    }

    public static JedisPool createPool() {
        GenericObjectPoolConfig<Jedis> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(ConfigUtil.getInt("redis.max.total", 8));
        poolConfig.setMaxIdle(ConfigUtil.getInt("redis.max.idle", 8));
        poolConfig.setMinIdle(ConfigUtil.getInt("redis.min.idle", 1));
        poolConfig.setTestOnBorrow(true);

        String host = ConfigUtil.get("redis.host", "localhost");
        int port = ConfigUtil.getInt("redis.port", 6379);
        int timeoutMs = ConfigUtil.getInt("redis.timeout.ms", 2000);

        return new JedisPool(poolConfig, host, port, timeoutMs);
    }

    public static String buildSongDimKey(String songId) {
        return SONG_DIM_KEY_PREFIX + songId;
    }

    public static String get(JedisPool jedisPool, String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(key);
        }
    }

    public static void setex(JedisPool jedisPool, String key, int ttlSeconds, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.setex(key, ttlSeconds, value);
        }
    }

    public static void delete(JedisPool jedisPool, String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(key);
        }
    }
}
