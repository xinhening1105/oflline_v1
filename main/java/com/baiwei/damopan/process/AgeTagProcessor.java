package com.baiwei.damopan.process;

import com.baiwei.damopan.model.UserBehavior;
import com.baiwei.damopan.model.UserTag;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 年龄标签处理器
 */
public class AgeTagProcessor extends KeyedProcessFunction<String, UserBehavior, UserTag> {

    private static final String[] AGE_GROUPS = {"18-24", "25-29", "30-34", "35-39", "40-49", "50以上"};

    @Override
    public void processElement(UserBehavior behavior, Context ctx, Collector<UserTag> out) throws Exception {
        UserTag userTag = new UserTag();
        userTag.setUserId(behavior.getUserId());

        // 简化逻辑：根据用户ID哈希确定年龄段
        int ageIndex = Math.abs(behavior.getUserId().hashCode()) % AGE_GROUPS.length;
        String ageGroup = AGE_GROUPS[ageIndex];

        userTag.addTag("age_group", ageGroup);
        userTag.addTag("age_confidence", 0.7);
        userTag.addTag("update_time", System.currentTimeMillis());

        out.collect(userTag);
    }
}