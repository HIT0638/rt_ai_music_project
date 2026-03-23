package com.music.app.dm;

import com.alibaba.fastjson.JSON;
import com.music.bean.AiOverviewStats;
import com.music.util.ConfigUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Timestamp;

public class DmAiOverviewApp {

    public static void main(String[] args) throws Exception {
        ConfigUtil.init(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableClosureCleaner();
        env.setParallelism(ConfigUtil.getInt("flink.parallelism", 2));

        String kafkaServer = ConfigUtil.get("kafka.bootstrap.servers", "localhost:9092");
        String dwsTopicName = ConfigUtil.get("kafka.dws.ai.overview", "dws_ai_overview_rt");
        String groupId = "dm-ai-overview-group";

        KafkaSource<String> dwsSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaServer)
            .setTopics(dwsTopicName)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<AiOverviewStats> statsStream = env
            .fromSource(dwsSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .name("DWS AI Overview Source")
            .map(value -> JSON.parseObject(value, AiOverviewStats.class))
            .name("Parse AI Overview Stats")
            .filter(DmAiOverviewApp::isValidRecord)
            .name("Filter Valid AI Overview Stats");

        String tableName = ConfigUtil.get("clickhouse.table.dm.ai.overview", "dm_ai_overview_10s");

        String insertSql = "insert into " + tableName
            + " (window_start, window_end, request_count, success_count, avg_latency_ms, "
            + "like_count, dislike_count, followup_count, click_count) "
            + "values (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        statsStream
            .addSink(JdbcSink.sink(
                insertSql,
                (ps, value) -> {
                    ps.setTimestamp(1, Timestamp.valueOf(value.getWindowStart()));
                    ps.setTimestamp(2, Timestamp.valueOf(value.getWindowEnd()));
                    ps.setLong(3, value.getRequestCount());
                    ps.setLong(4, value.getSuccessCount());
                    ps.setLong(5, value.getAvgLatencyMs());
                    ps.setLong(6, value.getLikeCount());
                    ps.setLong(7, value.getDislikeCount());
                    ps.setLong(8, value.getFollowupCount());
                    ps.setLong(9, value.getClickCount());
                },
                JdbcExecutionOptions.builder()
                    .withBatchSize(100)
                    .withBatchIntervalMs(1000)
                    .withMaxRetries(3)
                    .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl(ConfigUtil.get("clickhouse.url"))
                    .withPassword(ConfigUtil.get("clickhouse.password", ""))
                    .withUsername(ConfigUtil.get("clickhouse.user", "default"))
                    .withDriverName(ConfigUtil.get(
                        "clickhouse.driver",
                        "com.clickhouse.jdbc.ClickHouseDriver"))
                    .build()
            ))
            .name("ClickHouse AI Overview Sink");

        env.execute("DM AI Overview App");
    }

    private static boolean isValidRecord(AiOverviewStats value) {
        return value != null
            && value.getWindowStart() != null
            && value.getWindowEnd() != null
            && value.getRequestCount() != null
            && value.getSuccessCount() != null
            && value.getAvgLatencyMs() != null
            && value.getLikeCount() != null
            && value.getDislikeCount() != null
            && value.getFollowupCount() != null
            && value.getClickCount() != null;
    }
}
