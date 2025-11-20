package com.baiwei.damopan.process;

import com.baiwei.damopan.model.UserBehavior;
import com.baiwei.damopan.model.UserTag;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * 用户性别标签实时处理器
 */
public class GenderTagProcessor extends KeyedProcessFunction<String, UserBehavior, UserTag> {

    private transient ValueState<GenderStats> genderStatsState;

    // 性别相关品类定义
    private static final Set<String> FEMALE_CATEGORIES = new HashSet<>(Arrays.asList(
            "女装", "女鞋", "美容护肤", "女士内衣", "饰品", "孕妇装", "童装"
    ));

    private static final Set<String> MALE_CATEGORIES = new HashSet<>(Arrays.asList(
            "男装", "男鞋", "运动服", "运动鞋", "网游装备", "电玩"
    ));

    private static final Set<String> FAMILY_CATEGORIES = new HashSet<>(Arrays.asList(
            "家居用品", "厨房电器", "生活电器", "儿童用品", "家庭保健"
    ));

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<GenderStats> descriptor =
                new ValueStateDescriptor<>("gender-stats", GenderStats.class);
        genderStatsState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(UserBehavior behavior, Context ctx, Collector<UserTag> out) throws Exception {
        String userId = behavior.getUserId();

        // 获取当前统计状态
        GenderStats stats = genderStatsState.value();
        if (stats == null) {
            stats = new GenderStats();
        }

        // 更新性别统计
        updateGenderStats(stats, behavior);
        genderStatsState.update(stats);

        // 计算性别标签
        UserTag userTag = calculateGenderTag(userId, stats);
        if (userTag != null) {
            out.collect(userTag);
        }
    }

    private void updateGenderStats(GenderStats stats, UserBehavior behavior) {
        String category = behavior.getCategory();
        String eventType = behavior.getEventType();

        if (category == null || eventType == null) return;

        double weight = getBehaviorWeight(eventType);

        if (FEMALE_CATEGORIES.contains(category)) {
            stats.femaleScore += weight;
            stats.femaleBehaviors++;
        } else if (MALE_CATEGORIES.contains(category)) {
            stats.maleScore += weight;
            stats.maleBehaviors++;
        } else if (FAMILY_CATEGORIES.contains(category)) {
            stats.familyScore += weight;
            stats.familyBehaviors++;
        } else {
            stats.otherBehaviors++;
        }
        stats.totalBehaviors++;
    }

    private double getBehaviorWeight(String eventType) {
        switch (eventType) {
            case "purchase": return 1.0;
            case "cart": return 0.6;
            case "favorite": return 0.4;
            case "browse": return 0.3;
            default: return 0.1;
        }
    }

    private UserTag calculateGenderTag(String userId, GenderStats stats) {
        if (stats.totalBehaviors < 3) {
            return createUnknownTag(userId);
        }

        double totalScore = stats.femaleScore + stats.maleScore + stats.familyScore;
        if (totalScore <= 0) return createUnknownTag(userId);

        double femaleRatio = stats.femaleScore / totalScore;
        double maleRatio = stats.maleScore / totalScore;
        double familyRatio = stats.familyScore / totalScore;

        String genderTag;
        if (familyRatio >= 0.3 && Math.abs(femaleRatio - maleRatio) <= 0.1) {
            genderTag = "family";
        } else if (femaleRatio > maleRatio + 0.1) {
            genderTag = "female";
        } else if (maleRatio > femaleRatio + 0.1) {
            genderTag = "male";
        } else {
            genderTag = "unknown";
        }

        return createUserTag(userId, genderTag, femaleRatio, maleRatio, familyRatio, stats);
    }

    private UserTag createUserTag(String userId, String genderTag,
                                  double femaleRatio, double maleRatio,
                                  double familyRatio, GenderStats stats) {
        UserTag userTag = new UserTag();
        userTag.setUserId(userId);
        userTag.addTag("gender", genderTag);

        Map<String, Object> scores = new HashMap<>();
        scores.put("female", Math.round(femaleRatio * 1000) / 1000.0);
        scores.put("male", Math.round(maleRatio * 1000) / 1000.0);
        scores.put("family", Math.round(familyRatio * 1000) / 1000.0);
        userTag.addTag("gender_scores", scores);

        userTag.addTag("update_time", System.currentTimeMillis());
        return userTag;
    }

    private UserTag createUnknownTag(String userId) {
        UserTag userTag = new UserTag();
        userTag.setUserId(userId);
        userTag.addTag("gender", "unknown");
        userTag.addTag("update_time", System.currentTimeMillis());
        return userTag;
    }

    private static class GenderStats {
        double femaleScore = 0.0;
        double maleScore = 0.0;
        double familyScore = 0.0;
        int femaleBehaviors = 0;
        int maleBehaviors = 0;
        int familyBehaviors = 0;
        int otherBehaviors = 0;
        int totalBehaviors = 0;
    }
}