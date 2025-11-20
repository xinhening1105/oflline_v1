package com.baiwei.damopan.process;

import com.baiwei.damopan.model.UserProfile;
import com.baiwei.damopan.model.UserTag;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * 星座处理器
 */
public class ConstellationProcessor extends KeyedProcessFunction<String, UserProfile, UserTag> {

    private static final Map<String, int[]> CONSTELLATION_RANGES = new HashMap<>();
    static {
        CONSTELLATION_RANGES.put("摩羯座", new int[]{12, 22, 1, 19});
        CONSTELLATION_RANGES.put("水瓶座", new int[]{1, 20, 2, 18});
        CONSTELLATION_RANGES.put("双鱼座", new int[]{2, 19, 3, 20});
        CONSTELLATION_RANGES.put("白羊座", new int[]{3, 21, 4, 19});
        CONSTELLATION_RANGES.put("金牛座", new int[]{4, 20, 5, 20});
        CONSTELLATION_RANGES.put("双子座", new int[]{5, 21, 6, 21});
        CONSTELLATION_RANGES.put("巨蟹座", new int[]{6, 22, 7, 22});
        CONSTELLATION_RANGES.put("狮子座", new int[]{7, 23, 8, 22});
        CONSTELLATION_RANGES.put("处女座", new int[]{8, 23, 9, 22});
        CONSTELLATION_RANGES.put("天秤座", new int[]{9, 23, 10, 23});
        CONSTELLATION_RANGES.put("天蝎座", new int[]{10, 24, 11, 22});
        CONSTELLATION_RANGES.put("射手座", new int[]{11, 23, 12, 21});
    }

    @Override
    public void processElement(UserProfile profile, Context ctx, Collector<UserTag> out) throws Exception {
        UserTag userTag = new UserTag();
        userTag.setUserId(profile.getUserId());

        String constellation = "未知";
        if (profile.getBirthday() != null && !profile.getBirthday().isEmpty()) {
            try {
                constellation = calculateConstellation(profile.getBirthday());
            } catch (Exception e) {
                System.err.println("星座计算失败: " + e.getMessage());
            }
        }

        userTag.addTag("constellation", constellation);
        userTag.addTag("birthday", profile.getBirthday());
        userTag.addTag("update_time", System.currentTimeMillis());
        out.collect(userTag);
    }

    private String calculateConstellation(String birthday) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate date = LocalDate.parse(birthday, formatter);

        int month = date.getMonthValue();
        int day = date.getDayOfMonth();

        for (Map.Entry<String, int[]> entry : CONSTELLATION_RANGES.entrySet()) {
            String name = entry.getKey();
            int[] range = entry.getValue();

            if (isInRange(month, day, range)) {
                return name;
            }
        }
        return "未知";
    }

    private boolean isInRange(int month, int day, int[] range) {
        int startMonth = range[0];
        int startDay = range[1];
        int endMonth = range[2];
        int endDay = range[3];

        if (startMonth == 12 && endMonth == 1) {
            return (month == 12 && day >= startDay) || (month == 1 && day <= endDay);
        } else {
            if (month == startMonth) return day >= startDay;
            if (month == endMonth) return day <= endDay;
        }
        return false;
    }
}