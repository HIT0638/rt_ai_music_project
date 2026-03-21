package com.music.app.dm;

import com.alibaba.fastjson.JSON;
import com.music.bean.SingerHotStats;
import com.music.util.ConfigUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Timestamp;

public class DmSingerHotApp {

    public static void main(String[] args) throws Exception {
        ConfigUtil.init(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableClosureCleaner();
        env.setParallelism(ConfigUtil.getInt("flink.parallelism", 2));

        String kafkaServers = ConfigUtil.get("kafka.bootstrap.servers");
        String dwsTopicName = ConfigUtil.get("kafka.dws.singer.hot", "dws_singer_hot_rt");
        String groupId = "dm-singer-hot-group";

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaServers)
            .setTopics(dwsTopicName)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<SingerHotStats> statsStream = env
            .fromSource(kafkaSource, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Kafka Source")
            .name("DWS Singer Hot Source")
            .map(value -> JSON.parseObject(value, SingerHotStats.class))
            .name("Parse Singer Hot Stats")
            .filter(DmSingerHotApp::isValidRecord)
            .name("Filter Valid Singer Hot Stats");

        String tableName = ConfigUtil.get("clickhouse.table.dm.singer.hot", "dm_singer_hot_10s");
        String insertSql = "INSERT INTO " + tableName
            + " (window_start, window_end, singer_info, play_count) VALUES (?, ?, ?, ?)";

        statsStream
            .addSink(JdbcSink.sink(
                insertSql,
                (ps, value) -> {
                    ps.setTimestamp(1, Timestamp.valueOf(value.getWindowStart()));
                    ps.setTimestamp(2, Timestamp.valueOf(value.getWindowEnd()));
                    ps.setString(3, value.getSingerInfo());
                    ps.setLong(4, value.getPlayCount());
                },
                JdbcExecutionOptions.builder()
                    .withBatchSize(100)
                    .withBatchIntervalMs(1000)
                    .withMaxRetries(3)
                    .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl(ConfigUtil.get("clickhouse.url"))
                    .withDriverName(ConfigUtil.get("clickhouse.driver", "com.clickhouse.jdbc.ClickHouseDriver"))
                    .withUsername(ConfigUtil.get("clickhouse.user", "default"))
                    .withPassword(ConfigUtil.get("clickhouse.password", ""))
                    .build()
            ))
            .name("ClickHouse Singer Hot Sink");

        env.execute("DM Singer Hot App");
    }

    private static boolean isValidRecord(SingerHotStats value) {
        return value != null
            && value.getWindowStart() != null
            && value.getWindowEnd() != null
            && value.getSingerInfo() != null
            && value.getPlayCount() != null;
    }
}
