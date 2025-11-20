package com.baiwei.damopan.process;

import com.baiwei.damopan.model.UserProfile;
import com.baiwei.damopan.model.UserTag;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 身高体重处理器
 */
public class HeightWeightProcessor extends KeyedProcessFunction<String, UserProfile, UserTag> {

    @Override
    public void processElement(UserProfile profile, Context ctx, Collector<UserTag> out) throws Exception {
        UserTag userTag = new UserTag();
        userTag.setUserId(profile.getUserId());

        if (profile.getHeight() != null) {
            userTag.addTag("height", profile.getHeight());
        }
        if (profile.getWeight() != null) {
            userTag.addTag("weight", profile.getWeight());
        }

        // 计算BMI
        if (profile.getHeight() != null && profile.getWeight() != null && profile.getHeight() > 0) {
            double heightInMeter = profile.getHeight() / 100.0;
            double bmi = profile.getWeight() / (heightInMeter * heightInMeter);
            userTag.addTag("bmi", Math.round(bmi * 10) / 10.0);

            // BMI分类
            String bmiCategory;
            if (bmi < 18.5) bmiCategory = "偏瘦";
            else if (bmi < 24) bmiCategory = "正常";
            else if (bmi < 28) bmiCategory = "偏胖";
            else bmiCategory = "肥胖";
            userTag.addTag("bmi_category", bmiCategory);
        }

        userTag.addTag("update_time", System.currentTimeMillis());
        out.collect(userTag);
    }
}