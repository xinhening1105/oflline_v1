package com.baiwei.damopan.model;

import lombok.Data;

/**
 * 用户基础特征数据模型
 */
@Data
public class UserProfile {
    private String userId;
    private Integer age;            // 年龄
    private String ageGroup;        // 年龄段: 18-24,25-29等
    private String gender;          // 性别: male|female|family
    private Double height;          // 身高(cm)
    private Double weight;          // 体重(kg)
    private String constellation;   // 星座
    private String birthYear;       // 出生年代
    private String birthday;        // 生日日期
    private Long updateTime;        // 更新时间
    private String dataSource;      // 数据来源

    public UserProfile() {
        this.updateTime = System.currentTimeMillis();
    }
}