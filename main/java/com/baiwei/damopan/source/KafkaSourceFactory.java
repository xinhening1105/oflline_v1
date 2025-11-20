package com.baiwei.damopan.source;

import com.baiwei.damopan.model.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import com.alibaba.fastjson.JSONObject;

import java.util.Properties;

/**
 * Kafka数据源工厂类
 */
public class KafkaSourceFactory {

    public static DataStream<UserBehavior> createUserBehaviorStream(StreamExecutionEnvironment env) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "damopan-group");
        props.setProperty("auto.offset.reset", "latest");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "user-behavior-topic",
                new SimpleStringSchema(),
                props
        );
        consumer.setStartFromLatest();

        return env.addSource(consumer)
                .map(new UserBehaviorMapFunction())
                .name("kafka-user-behavior-source");
    }

    private static class UserBehaviorMapFunction implements MapFunction<String, UserBehavior> {
        @Override
        public UserBehavior map(String value) throws Exception {
            try {
                JSONObject json = JSONObject.parseObject(value);
                UserBehavior behavior = new UserBehavior();

                behavior.setUserId(json.getString("userId"));
                behavior.setCategory(json.getString("category"));
                behavior.setEventType(json.getString("eventType"));
                behavior.setBrand(json.getString("brand"));
                behavior.setPrice(json.getDouble("price"));
                behavior.setTimestamp(json.getString("timestamp"));

                return behavior;
            } catch (Exception e) {
                System.err.println("JSON解析错误: " + e.getMessage());
                UserBehavior errorBehavior = new UserBehavior();
                errorBehavior.setUserId("parse_error");
                return errorBehavior;
            }
        }
    }
}