package com.music.app.dm;

import com.alibaba.fastjson.JSON;
import com.music.bean.SongHotStats;
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

public class DmSongHotApp {

    public static void main(String[] args) throws Exception {
        ConfigUtil.init(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableClosureCleaner();
        env.setParallelism(ConfigUtil.getInt("flink.parallelism", 2));

        String kafkaServers = ConfigUtil.get("kafka.bootstrap.servers");
        String dwsTopicName = ConfigUtil.get("kafka.dws.song.hot", "dws_song_hot_rt");
        String groupId = "dm-song-hot-group";

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaServers)
            .setTopics(dwsTopicName)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<SongHotStats> statsStream = env
            .fromSource(kafkaSource, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Kafka Source")
            .name("DWS Song Hot Source")
            .map(value -> JSON.parseObject(value, SongHotStats.class))
            .name("Parse Song Hot Stats")
            .filter(DmSongHotApp::isValidRecord)
            .name("Filter Valid Song Hot Stats");

        String tableName = ConfigUtil.get("clickhouse.table.dm.song.hot", "dm_song_hot_10s");
        String insertSql = "INSERT INTO " + tableName
            + " (window_start, window_end, song_id, song_name, play_count) VALUES (?, ?, ?, ?, ?)";

        statsStream
            .addSink(JdbcSink.sink(
                insertSql,
                (ps, value) -> {
                    ps.setTimestamp(1, Timestamp.valueOf(value.getWindowStart()));
                    ps.setTimestamp(2, Timestamp.valueOf(value.getWindowEnd()));
                    ps.setString(3, value.getSongId());
                    ps.setString(4, value.getSongName());
                    ps.setLong(5, value.getPlayCount());
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
            .name("ClickHouse Song Hot Sink");

        env.execute("DM Song Hot App");
    }

    private static boolean isValidRecord(SongHotStats value) {
        return value != null
            && value.getWindowStart() != null
            && value.getWindowEnd() != null
            && value.getSongId() != null
            && value.getSongName() != null
            && value.getPlayCount() != null;
    }
}
