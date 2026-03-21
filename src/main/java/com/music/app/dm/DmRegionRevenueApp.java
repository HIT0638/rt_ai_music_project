package com.music.app.dm;

import com.alibaba.fastjson.JSON;
import com.music.bean.RegionRevenueStats;
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

public class DmRegionRevenueApp {

    public static void main(String[] args) throws Exception {
        ConfigUtil.init(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableClosureCleaner();
        env.setParallelism(ConfigUtil.getInt("flink.parallelism", 2));

        String kafkaServers = ConfigUtil.get("kafka.bootstrap.servers");
        String dwsTopicName = ConfigUtil.get(
            "kafka.dws.region.revenue",
            "dws_region_revenue_rt");
        String groupId = "dm-region-revenue-group";

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaServers)
            .setTopics(dwsTopicName)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<RegionRevenueStats> statsStream = env
            .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .name("DWS Region Revenue Source")
            .map(value -> JSON.parseObject(value, RegionRevenueStats.class))
            .name("Parse Region Revenue Stats")
            .filter(DmRegionRevenueApp::isValidRecord)
            .name("Filter Valid Region Revenue Stats");

        String tableName = ConfigUtil.get(
            "clickhouse.table.dm.region.revenue",
            "dm_region_revenue_10s");

        // TODO: 如果后面要把 totalAmount 从“分”转换成“元”，统一在 DWS 或 DM 层选一个地方做，不要两边都转。
        String insertSql = "INSERT INTO " + tableName
            + " (window_start, window_end, province, city, order_count, total_amount)"
            + " VALUES (?, ?, ?, ?, ?, ?)";

        statsStream
            .addSink(JdbcSink.sink(
                insertSql,
                (ps, value) -> {
                    ps.setTimestamp(1, Timestamp.valueOf(value.getWindowStart()));
                    ps.setTimestamp(2, Timestamp.valueOf(value.getWindowEnd()));
                    ps.setString(3, value.getProvince());
                    ps.setString(4, value.getCity());
                    ps.setLong(5, value.getOrderCount());
                    ps.setLong(6, value.getTotalAmount());
                },
                JdbcExecutionOptions.builder()
                    .withBatchSize(100)
                    .withBatchIntervalMs(1000)
                    .withMaxRetries(3)
                    .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl(ConfigUtil.get("clickhouse.url"))
                    .withDriverName(ConfigUtil.get(
                        "clickhouse.driver",
                        "com.clickhouse.jdbc.ClickHouseDriver"))
                    .withUsername(ConfigUtil.get("clickhouse.user", "default"))
                    .withPassword(ConfigUtil.get("clickhouse.password", ""))
                    .build()
            ))
            .name("ClickHouse Region Revenue Sink");

        env.execute("DM Region Revenue App");
    }

    private static boolean isValidRecord(RegionRevenueStats value) {
        return value != null
            && value.getWindowStart() != null
            && value.getWindowEnd() != null
            && value.getProvince() != null
            && value.getCity() != null
            && value.getOrderCount() != null
            && value.getTotalAmount() != null;
    }
}
