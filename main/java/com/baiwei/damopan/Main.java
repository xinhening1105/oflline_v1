package com.baiwei.damopan;

import com.baiwei.damopan.model.UserBehavior;
import com.baiwei.damopan.model.UserProfile;
import com.baiwei.damopan.model.UserTag;
import com.baiwei.damopan.process.*;
import com.baiwei.damopan.source.KafkaSourceFactory;
import com.baiwei.damopan.source.MockUserBehaviorSource;
import com.baiwei.damopan.source.MockUserProfileSource;
import com.baiwei.damopan.sink.RedisSink;
import com.baiwei.damopan.sink.ConsoleSink;
import com.baiwei.damopan.sink.FileSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * 工单编号：大数据-用户画像-11-达摩盘基础特征
 * 达摩盘基础特征实时处理主程序
 *
 * 功能描述：
 * 1. 实时处理用户行为数据和档案数据
 * 2. 生成用户基础特征标签（年龄、性别、身高体重、星座）
 * 3. 支持多种数据输出方式（控制台、文件、Redis）
 * 4. 具备完善的错误处理和监控机制
 */
public class Main {

    private static final String JOB_NAME = "Damopan-Basic-Features-Realtime-Job";
    private static final int CHECKPOINT_INTERVAL = 5000; // 5秒
    private static final int PARALLELISM = 2; // 并行度

    public static void main(String[] args) throws Exception {
        System.out.println("=== 达摩盘基础特征实时处理系统启动 ===");
        System.out.println("工单编号：大数据-用户画像-11-达摩盘基础特征");
        System.out.println("作业名称: " + JOB_NAME);
        System.out.println("启动时间: " + java.time.LocalDateTime.now());

        try {
            // 1. 创建执行环境
            StreamExecutionEnvironment env = createExecutionEnvironment();

            // 2. 构建处理管道
            buildRealtimePipeline(env);

            // 3. 执行作业
            System.out.println("开始执行Flink作业...");
            env.execute(JOB_NAME);

            System.out.println("=== 系统正常启动完成 ===");

        } catch (Exception e) {
            System.err.println("!!! 系统启动失败 !!!");
            System.err.println("错误信息: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * 创建并配置执行环境
     */
    private static StreamExecutionEnvironment createExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 基础配置
        env.setParallelism(PARALLELISM);

        // 检查点配置（用于容错）
        env.enableCheckpointing(CHECKPOINT_INTERVAL);

        // 重启策略配置
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 最大重启次数
                Time.of(10, TimeUnit.SECONDS) // 重启间隔
        ));

        // 状态后端配置（生产环境需要配置）
        // env.setStateBackend(new HashMapStateBackend());
        // env.getCheckpointConfig().setCheckpointStorage("hdfs:///checkpoints");

        System.out.println("执行环境配置完成");
        System.out.println("并行度: " + PARALLELISM);
        System.out.println("检查点间隔: " + CHECKPOINT_INTERVAL + "ms");

        return env;
    }

    /**
     * 构建实时处理管道
     */
    private static void buildRealtimePipeline(StreamExecutionEnvironment env) {
        System.out.println("开始构建实时处理管道...");

        // ==================== 数据源部分 ====================
        System.out.println("1. 初始化数据源...");

        // 用户行为数据流 - 使用模拟数据源
        DataStream<UserBehavior> behaviorStream = env
                .addSource(new MockUserBehaviorSource(), "mock-user-behavior-source")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                )
                .setParallelism(1)
                .name("user-behavior-stream");

        // 用户档案数据流 - 使用模拟数据源
        DataStream<UserProfile> profileStream = env
                .addSource(new MockUserProfileSource(), "mock-user-profile-source")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserProfile>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                )
                .setParallelism(1)
                .name("user-profile-stream");

        // ==================== 数据处理部分 ====================
        System.out.println("2. 配置数据处理算子...");

        // 年龄标签处理
        DataStream<UserTag> ageTags = behaviorStream
                .keyBy(UserBehavior::getUserId)
                .process(new AgeTagProcessor())
                .name("age-tag-processor")
                .setParallelism(2);

        // 性别标签处理
        DataStream<UserTag> genderTags = behaviorStream
                .keyBy(UserBehavior::getUserId)
                .process(new GenderTagProcessor())
                .name("gender-tag-processor")
                .setParallelism(2);

        // 身高体重处理
        DataStream<UserTag> heightWeightTags = profileStream
                .keyBy(UserProfile::getUserId)
                .process(new HeightWeightProcessor())
                .name("height-weight-processor")
                .setParallelism(2);

        // 星座标签处理
        DataStream<UserTag> constellationTags = profileStream
                .keyBy(UserProfile::getUserId)
                .process(new ConstellationProcessor())
                .name("constellation-processor")
                .setParallelism(2);

        // ==================== 数据合并部分 ====================
        System.out.println("3. 合并标签数据流...");

        // 合并所有标签流
        DataStream<UserTag> allTags = ageTags
                .union(genderTags, heightWeightTags, constellationTags)
                .name("all-tags-union")
                .setParallelism(2);

        // ==================== 数据输出部分 ====================
        System.out.println("4. 配置数据输出...");

        // 输出到控制台（调试用）
        allTags
                .addSink(new ConsoleSink())
                .name("console-sink")
                .setParallelism(1);

        // 输出到文件（备份用）
        allTags
                .addSink(new FileSink("/tmp/damopan_tags/output.txt"))
                .name("file-sink")
                .setParallelism(1);

        // 输出到Redis（生产环境用）- 可选，根据实际情况启用
        boolean enableRedis = false; // 根据配置决定是否启用
        if (enableRedis) {
            try {
                allTags
                        .addSink(new RedisSink("localhost", 6379, ""))
                        .name("redis-sink")
                        .setParallelism(2);
                System.out.println("Redis输出已启用");
            } catch (Exception e) {
                System.err.println("Redis连接失败: " + e.getMessage());
            }
        } else {
            System.out.println("Redis输出未启用");
        }

        // ==================== 监控和指标部分 ====================
        System.out.println("5. 配置监控指标...");

        // 添加数据流监控
        addStreamMonitoring(behaviorStream, profileStream, allTags);

        System.out.println("实时处理管道构建完成 ✓");
        printPipelineSummary();
    }

    /**
     * 添加数据流监控
     */
    private static void addStreamMonitoring(
            DataStream<UserBehavior> behaviorStream,
            DataStream<UserProfile> profileStream,
            DataStream<UserTag> allTags) {

        // 行为数据流监控
        behaviorStream
                .map(behavior -> {
                    System.out.println("[监控] 行为数据: " + behavior.getUserId() + " - " + behavior.getBehaviorType());
                    return behavior;
                })
                .name("behavior-monitor")
                .setParallelism(1);

        // 标签数据流监控
        allTags
                .map(tag -> {
                    System.out.println("[监控] 生成标签: " + tag.getUserId() + " - " + tag.getTagType() + " = " + tag.getTagValue());
                    return tag;
                })
                .name("tag-monitor")
                .setParallelism(1);
    }

    /**
     * 打印管道摘要信息
     */
    private static void printPipelineSummary() {
        System.out.println("\n=== 处理管道配置摘要 ===");
        System.out.println("数据源:");
        System.out.println("  - 用户行为数据 (MockUserBehaviorSource)");
        System.out.println("  - 用户档案数据 (MockUserProfileSource)");
        System.out.println("\n处理算子:");
        System.out.println("  - 年龄标签处理器 (AgeTagProcessor)");
        System.out.println("  - 性别标签处理器 (GenderTagProcessor)");
        System.out.println("  - 身高体重处理器 (HeightWeightProcessor)");
        System.out.println("  - 星座标签处理器 (ConstellationProcessor)");
        System.out.println("\n数据输出:");
        System.out.println("  - 控制台输出 (ConsoleSink)");
        System.out.println("  - 文件输出 (FileSink)");
        System.out.println("  - Redis输出 (可选)");
        System.out.println("\n监控指标:");
        System.out.println("  - 行为数据监控");
        System.out.println("  - 标签数据监控");
        System.out.println("========================\n");
    }

    /**
     * 优雅关闭钩子
     */
    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n=== 系统正在关闭 ===");
            System.out.println("关闭时间: " + java.time.LocalDateTime.now());
            System.out.println("达摩盘特征处理系统已停止");
        }));
    }
}