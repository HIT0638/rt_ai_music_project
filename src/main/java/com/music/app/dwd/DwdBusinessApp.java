package com.music.app.dwd;

import com.music.bean.TblConfig;
import com.music.function.BusinessBroadcastProcessFunction;
import com.music.function.ConfigSourceFunction;
import com.music.function.DynamicKafkaSerializationSchema;
import com.music.util.ConfigUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DWD 层业务数据清洗主程序
 *
 * 功能：
 * 1. 从 MySQL 读取 config_info 配置表，构建广播流
 * 2. 从 Kafka 消费 ODS 层业务数据
 * 3. 根据配置动态路由数据到不同的 DWD Topic
 */
public class DwdBusinessApp {

    private static final Logger logger = LoggerFactory.getLogger(DwdBusinessApp.class);

    public static void main(String[] args) throws Exception {
        // 1. 初始化配置
        ConfigUtil.init(args);
        logger.info("配置初始化完成");

        // 2. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ConfigUtil.getInt("flink.parallelism", 2));
        logger.info("执行环境创建完成，并行度: {}", env.getParallelism());

        // 3. 创建配置流（广播流）
        DataStream<TblConfig> configStream = env
            .addSource(new ConfigSourceFunction())
            .name("Config Source");

        // 4. 定义广播状态描述符
        MapStateDescriptor<String, TblConfig> configStateDescriptor =
            new MapStateDescriptor<>(
                "configState",
                String.class,
                TblConfig.class
            );

        // 5. 广播配置流
        BroadcastStream<TblConfig> broadcastStream = configStream.broadcast(configStateDescriptor);
        logger.info("广播流创建完成");

        // 6. 创建主流（业务数据流）
        String kafkaServers = ConfigUtil.get("kafka.bootstrap.servers");
        String odsTopicName = ConfigUtil.get("kafka.ods.business.data");
        String groupId = "dwd-business-group";

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaServers)
            .setTopics(odsTopicName)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> mainStream = env
            .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .name("ODS Business Data Source");

        logger.info("主流创建完成，Topic: {}", odsTopicName);

        // 7. 连接主流和广播流
        SingleOutputStreamOperator<String> resultStream = mainStream
            .connect(broadcastStream)
            .process(new BusinessBroadcastProcessFunction(configStateDescriptor))
            .name("Broadcast Process");

        logger.info("主流和广播流连接完成");

        // 8. 写入 Kafka（动态路由）
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
            .setBootstrapServers(kafkaServers)
            .setRecordSerializer(new DynamicKafkaSerializationSchema())
            .build();

        resultStream
            .sinkTo(kafkaSink)
            .name("Dynamic Kafka Sink");

        logger.info("Kafka Sink 创建完成");

        // 9. 执行任务
        env.execute("DWD Business Data Cleaning");
    }
}
