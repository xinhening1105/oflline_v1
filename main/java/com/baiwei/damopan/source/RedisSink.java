package com.baiwei.damopan.sink;

import com.baiwei.damopan.model.UserTag;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Redis输出Sink
 */
public class RedisSink extends RichSinkFunction<UserTag> {

    private transient JedisPool jedisPool;
    private String redisHost;
    private int redisPort;
    private String password;

    public RedisSink() {
        this("localhost", 6379, "");
    }

    public RedisSink(String host, int port, String password) {
        this.redisHost = host;
        this.redisPort = port;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(100);
        config.setMaxIdle(20);
        config.setMinIdle(5);

        if (password != null && !password.isEmpty()) {
            jedisPool = new JedisPool(config, redisHost, redisPort, 3000, password);
        } else {
            jedisPool = new JedisPool(config, redisHost, redisPort, 3000);
        }
    }

    @Override
    public void invoke(UserTag userTag, Context context) throws Exception {
        try (Jedis jedis = jedisPool.getResource()) {
            String userId = userTag.getUserId();
            String key = "user:tag:" + userId;

            // 存储基础标签
            userTag.getBasicTags().forEach((tagName, tagValue) -> {
                jedis.hset(key, tagName, tagValue.toString());
            });

            // 存储标签得分
            userTag.getTagScores().forEach((dimension, score) -> {
                jedis.hset(key + ":scores", dimension, score.toString());
            });

            // 存储元数据
            jedis.hset(key, "update_time", userTag.getTimestamp().toString());
            jedis.hset(key, "version", userTag.getVersion().toString());

        } catch (Exception e) {
            System.err.println("Redis存储失败: " + e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        if (jedisPool != null) {
            jedisPool.close();
        }
        super.close();
    }
}