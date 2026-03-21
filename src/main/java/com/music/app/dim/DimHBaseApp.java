package com.music.app.dim;

import com.music.function.DimHBaseProcessFunction;
import com.music.util.ConfigUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DIM 层 HBase 写入主程序
 *
 * 功能：
 * 1. 从 Kafka 消费 dwd_dim_info_rt 数据
 * 2. 按 phoenix_table_name keyBy
 * 3. 使用 DimHBaseProcessFunction 处理数据
 * 4. 动态创建 Phoenix 表
 * 5. Upsert/Delete 数据到 HBase
 */
public class DimHBaseApp {

    private static final Logger logger = LoggerFactory.getLogger(DimHBaseApp.class);

    public static void main(String[] args) throws Exception {
        // 1. 初始化配置
        ConfigUtil.init(args);
        logger.info("配置初始化完成");

        // 2. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ConfigUtil.getInt("flink.parallelism", 2));
        logger.info("执行环境创建完成，并行度: {}", env.getParallelism());

        // 3. 创建 Kafka Source
        String kafkaServers = ConfigUtil.get("kafka.bootstrap.servers");
        String dwdTopicName = ConfigUtil.get("kafka.dwd.dim.info", "dwd_dim_info_rt");
        String groupId = "dim-hbase-group";

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaServers)
            .setTopics(dwdTopicName)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> dataStream = env
            .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .name("DWD Dim Info Source");

        logger.info("Kafka Source 创建完成，Topic: {}", dwdTopicName);

        // 4. 按 phoenix_table_name keyBy
        DataStream<Void> resultStream = dataStream
            .keyBy(value -> {
                // 提取 phoenix_table_name 作为 key
                try {
                    com.alibaba.fastjson.JSONObject json = com.alibaba.fastjson.JSON.parseObject(value);
                    return json.getString("phoenix_table_name");
                } catch (Exception e) {
                    logger.error("解析 phoenix_table_name 失败: {}", value, e);
                    return "unknown";
                }
            })
            .process(new DimHBaseProcessFunction())
            .name("Dim HBase Process");

        logger.info("KeyBy 和 Process 创建完成");

        // 5. 执行任务
        env.execute("DIM HBase Data Writing");
    }
}
