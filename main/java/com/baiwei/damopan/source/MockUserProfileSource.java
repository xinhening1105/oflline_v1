package com.baiwei.damopan.source;

import com.baiwei.damopan.model.UserProfile;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 模拟用户档案数据源
 */
public class MockUserProfileSource implements SourceFunction<UserProfile> {

    private volatile boolean isRunning = true;
    private final Random random = new Random();

    private final String[] userIds = {"user001", "user002", "user003", "user004", "user005"};
    private final String[] birthdays = {"1990-05-15", "1985-12-20", "1995-08-10", "1988-03-25", "1992-11-05"};
    private final Double[] heights = {165.0, 170.0, 175.0, 180.0, 185.0};
    private final Double[] weights = {55.0, 65.0, 70.0, 75.0, 80.0};

    @Override
    public void run(SourceContext<UserProfile> ctx) throws Exception {
        while (isRunning) {
            Thread.sleep(2000); // 每2秒一条数据

            UserProfile profile = new UserProfile();
            int index = random.nextInt(userIds.length);

            profile.setUserId(userIds[index]);
            profile.setBirthday(birthdays[index]);
            profile.setHeight(heights[random.nextInt(heights.length)]);
            profile.setWeight(weights[random.nextInt(weights.length)]);
            profile.setDataSource("mock");

            ctx.collect(profile);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}