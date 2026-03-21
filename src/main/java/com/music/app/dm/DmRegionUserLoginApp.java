package com.music.app.dm;

import com.alibaba.fastjson.JSON;
import com.music.bean.RegionLoginStats;
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

public class DmRegionUserLoginApp {

    public static void main(String[] args) throws Exception {
        ConfigUtil.init(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableClosureCleaner();
        env.setParallelism(ConfigUtil.getInt("flink.parallelism", 2));

        String kafkaServers = ConfigUtil.get("kafka.bootstrap.servers");
        String dwsTopicName = ConfigUtil.get(
            "kafka.dws.region.user.login",
            "dws_region_user_login_rt");
        String groupId = "dm-region-user-login-group";

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaServers)
            .setTopics(dwsTopicName)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<RegionLoginStats> statsStream = env
            .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .name("DWS Region User Login Source")
            .map(value -> JSON.parseObject(value, RegionLoginStats.class))
            .name("Parse Region Login Stats")
            .filter(DmRegionUserLoginApp::isValidRecord)
            .name("Filter Valid Region Login Stats");

        String tableName = ConfigUtil.get(
            "clickhouse.table.dm.region.user.login",
            "dm_region_user_login_10s");

        // TODO: 如果你后面把 DWS 统计粒度改成 city / province，
        // 这里的 CK 表结构和 INSERT 列顺序也要一起调整。
        String insertSql = "INSERT INTO " + tableName
            + " (window_start, window_end, province, city, district, adcode, login_user_count)"
            + " VALUES (?, ?, ?, ?, ?, ?, ?)";

        statsStream
            .addSink(JdbcSink.sink(
                insertSql,
                (ps, value) -> {
                    ps.setTimestamp(1, Timestamp.valueOf(value.getWindowStart()));
                    ps.setTimestamp(2, Timestamp.valueOf(value.getWindowEnd()));
                    ps.setString(3, value.getProvince());
                    ps.setString(4, value.getCity());
                    ps.setString(5, value.getDistrict());
                    ps.setString(6, value.getAdcode());
                    ps.setLong(7, value.getLoginUserCount());
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
            .name("ClickHouse Region User Login Sink");

        env.execute("DM Region User Login App");
    }

    private static boolean isValidRecord(RegionLoginStats value) {
        return value != null
            && value.getWindowStart() != null
            && value.getWindowEnd() != null
            && value.getProvince() != null
            && value.getCity() != null
            && value.getDistrict() != null
            && value.getAdcode() != null
            && value.getLoginUserCount() != null;
    }
}
