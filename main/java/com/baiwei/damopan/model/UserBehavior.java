package com.baiwei.damopan.model;

import lombok.Data;
import java.util.HashMap;
import java.util.Map;

/**
 * 用户行为数据模型
 */
@Data
public class UserBehavior {
    private String userId;          // 用户ID
    private String eventType;       // 事件类型: browse|cart|purchase|search
    private String category;        // 商品类目
    private String brand;           // 品牌
    private Double price;           // 价格
    private String searchKeywords;  // 搜索关键词
    private String timestamp;       // 事件时间
    private String device;          // 设备信息
    private Map<String, Object> extraInfo; // 扩展信息

    public UserBehavior() {
        this.extraInfo = new HashMap<>();
    }
}