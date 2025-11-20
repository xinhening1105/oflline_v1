package com.baiwei.damopan.util;

import java.util.Map;

/**
 * 工单编号：大数据-用户画像-11-达摩盘基础特征
 * 权重计算工具类
 */
public class WeightCalculator {

    /**
     * 获取行为权重
     */
    public static double getBehaviorWeight(String eventType) {
        if (eventType == null) return 0.1;

        switch (eventType.toLowerCase()) {
            case "purchase": return 0.5;   // 购买行为权重50%
            case "cart": return 0.3;       // 加购行为权重30%
            case "favorite": return 0.2;   // 收藏行为权重20%
            case "browse": return 0.1;     // 浏览行为权重10%
            case "search": return 0.15;    // 搜索行为权重15%
            default: return 0.1;
        }
    }

    /**
     * 计算综合得分
     */
    public static double calculateCompositeScore(Map<String, Double> scores, Map<String, Double> weights) {
        return scores.entrySet().stream()
                .mapToDouble(entry -> entry.getValue() * weights.getOrDefault(entry.getKey(), 0.0))
                .sum();
    }

    /**
     * 计算年龄维度权重
     */
    public static Map<String, Double> getAgeDimensionWeights() {
        return Map.of(
                "category", 0.3,   // 类目偏好
                "brand", 0.2,      // 品牌偏好
                "price", 0.15,     // 价格敏感度
                "time", 0.1,       // 时间行为
                "search", 0.1,     // 搜索词分析
                "social", 0.1,     // 社交互动
                "device", 0.05     // 设备信息
        );
    }

    /**
     * 计算数据源权重
     */
    public static Map<String, Double> getDataSourceWeights() {
        return Map.of(
                "device", 1.0,           // 智能设备数据
                "verified", 0.9,         // 实名认证信息
                "recent_shopping", 0.85, // 近期购物数据
                "historical_shopping", 0.7, // 历史购物数据
                "customer_service", 0.6  // 客服沟通记录
        );
    }

    /**
     * 计算性别分类置信度
     */
    public static double calculateGenderConfidence(double femaleRatio, double maleRatio, double familyRatio) {
        double maxScore = Math.max(femaleRatio, Math.max(maleRatio, familyRatio));
        double secondMax = getSecondMax(femaleRatio, maleRatio, familyRatio);
        return Math.round((maxScore - secondMax) * 1000) / 1000.0; // 保留3位小数
    }

    /**
     * 获取第二大的分数
     */
    private static double getSecondMax(double a, double b, double c) {
        if (a >= b && a >= c) {
            return Math.max(b, c);
        } else if (b >= a && b >= c) {
            return Math.max(a, c);
        } else {
            return Math.max(a, b);
        }
    }

    /**
     * 标准化得分（0-1范围）
     */
    public static double normalizeScore(double score, double min, double max) {
        if (max == min) return 0.5;
        return (score - min) / (max - min);
    }

    /**
     * 计算加权平均值
     */
    public static double calculateWeightedAverage(Map<Double, Double> valuesWithWeights) {
        double totalWeight = 0.0;
        double weightedSum = 0.0;

        for (Map.Entry<Double, Double> entry : valuesWithWeights.entrySet()) {
            double value = entry.getKey();
            double weight = entry.getValue();
            weightedSum += value * weight;
            totalWeight += weight;
        }

        return totalWeight > 0 ? weightedSum / totalWeight : 0.0;
    }

    /**
     * 计算欧几里得距离（用于相似度计算）
     */
    public static double calculateEuclideanDistance(double[] vector1, double[] vector2) {
        if (vector1.length != vector2.length) {
            throw new IllegalArgumentException("向量维度必须相同");
        }

        double sum = 0.0;
        for (int i = 0; i < vector1.length; i++) {
            double diff = vector1[i] - vector2[i];
            sum += diff * diff;
        }

        return Math.sqrt(sum);
    }

    /**
     * 计算余弦相似度
     */
    public static double calculateCosineSimilarity(double[] vector1, double[] vector2) {
        if (vector1.length != vector2.length) {
            throw new IllegalArgumentException("向量维度必须相同");
        }

        double dotProduct = 0.0;
        double norm1 = 0.0;
        double norm2 = 0.0;

        for (int i = 0; i < vector1.length; i++) {
            dotProduct += vector1[i] * vector2[i];
            norm1 += vector1[i] * vector1[i];
            norm2 += vector2[i] * vector2[i];
        }

        if (norm1 == 0 || norm2 == 0) {
            return 0.0;
        }

        return dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));
    }
}