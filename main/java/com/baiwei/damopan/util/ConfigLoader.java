package com.baiwei.damopan.util;

import java.io.InputStream;
import java.util.Properties;

/**
 * 配置加载工具类
 */
public class ConfigLoader {

    public static Properties load(String configFile) {
        Properties props = new Properties();

        try (InputStream input = ConfigLoader.class.getClassLoader()
                .getResourceAsStream(configFile)) {

            if (input == null) {
                System.out.println("配置文件 " + configFile + " 未找到，使用默认配置");
                return getDefaultConfig();
            }

            props.load(input);
            System.out.println("成功加载配置文件: " + configFile);

        } catch (Exception e) {
            System.err.println("加载配置文件失败: " + e.getMessage());
            return getDefaultConfig();
        }

        return props;
    }

    private static Properties getDefaultConfig() {
        Properties props = new Properties();
        props.setProperty("kafka.bootstrap.servers", "localhost:9092");
        props.setProperty("kafka.group.id", "damopan-realtime");
        props.setProperty("redis.host", "localhost");
        props.setProperty("redis.port", "6379");
        props.setProperty("redis.password", "");
        return props;
    }
}