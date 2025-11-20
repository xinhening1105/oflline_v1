package com.baiwei.damopan.source;

import com.baiwei.damopan.model.UserBehavior;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 模拟用户行为数据源
 */
public class MockUserBehaviorSource implements SourceFunction<UserBehavior> {

    private volatile boolean isRunning = true;
    private final Random random = new Random();

    private final String[] userIds = {"user001", "user002", "user003", "user004", "user005"};
    private final String[] categories = {"女装", "男装", "家居用品", "美容护肤", "运动鞋", "儿童用品"};
    private final String[] events = {"browse", "cart", "purchase", "favorite"};
    private final String[] brands = {"ZARA", "优衣库", "苹果", "小米", "海尔"};

    @Override
    public void run(SourceContext<UserBehavior> ctx) throws Exception {
        while (isRunning) {
            Thread.sleep(1000); // 每秒一条数据

            UserBehavior behavior = new UserBehavior();
            behavior.setUserId(userIds[random.nextInt(userIds.length)]);
            behavior.setCategory(categories[random.nextInt(categories.length)]);
            behavior.setEventType(events[random.nextInt(events.length)]);
            behavior.setBrand(brands[random.nextInt(brands.length)]);
            behavior.setPrice(random.nextDouble() * 1000);
            behavior.setTimestamp(String.valueOf(System.currentTimeMillis()));

            ctx.collect(behavior);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}