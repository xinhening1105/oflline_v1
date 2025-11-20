package com.baiwei.damopan.model;

import lombok.Data;
import java.util.HashMap;
import java.util.Map;

/**
 * 用户标签输出模型
 */
@Data
public class UserTag {
    private String userId;
    private Map<String, Object> basicTags;  // 基础标签
    private Map<String, Double> tagScores;  // 标签得分
    private Long timestamp;
    private Integer version;        // 标签版本

    public UserTag() {
        this.basicTags = new HashMap<>();
        this.tagScores = new HashMap<>();
        this.timestamp = System.currentTimeMillis();
        this.version = 1;
    }

    public void addTag(String tagName, Object tagValue) {
        basicTags.put(tagName, tagValue);
    }

    public void addScore(String dimension, Double score) {
        tagScores.put(dimension, score);
    }
}