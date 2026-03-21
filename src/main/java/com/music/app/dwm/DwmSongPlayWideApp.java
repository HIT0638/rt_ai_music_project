package com.music.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.music.util.ConfigUtil;
import com.music.util.RedisUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class DwmSongPlayWideApp {

    private static final Logger logger = LoggerFactory.getLogger(DwmSongPlayWideApp.class);
    private static final String PHOENIX_URL = "jdbc:phoenix:localhost:2181";
    private static final String SONG_LOOKUP_SQL =
        "SELECT SOURCE_ID, NAME, ALBUM, SINGER_INFO, POST_TIME, AUTHORIZED_COMPANY " +
        "FROM DIM_SONG WHERE SOURCE_ID = ?";

    public static void main(String[] args) throws Exception {
        ConfigUtil.init(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ConfigUtil.getInt("flink.parallelism", 2));

        String kafkaServers = ConfigUtil.get("kafka.bootstrap.servers");
        String dwdTopicName = ConfigUtil.get("kafka.dwd.user.playsong");
        String dwmTopicName = ConfigUtil.get("kafka.dwm.song.play.wide", "dwm_song_play_wide_rt");
        String groupId = "dwm-song-play-wide-group";

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaServers)
            .setGroupId(groupId)
            .setTopics(dwdTopicName)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> sourceStream = env
            .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .name("DWD User Play Song Source");

        DataStream<String> wideStream = sourceStream
            .map(new SongDimEnrichmentFunction())
            .name("Song Dim Enrichment");

        wideStream
            .sinkTo(createKafkaSink(kafkaServers, dwmTopicName))
            .name("DWM Song Play Wide Sink");

        env.execute("DWM Song Play Wide App");
    }

    private static KafkaSink<String> createKafkaSink(String bootstrapServers, String topic) {
        return KafkaSink.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(topic)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build()
            )
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
    }

    private static class SongDimEnrichmentFunction extends RichMapFunction<String, String> {

        private transient Connection conn;
        private transient PreparedStatement pst;
        private transient JedisPool jedisPool;
        private int cacheTtlSeconds;

        @Override
        public void open(Configuration parameters) throws Exception {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(PHOENIX_URL);
            pst = conn.prepareStatement(SONG_LOOKUP_SQL);
            jedisPool = RedisUtil.createPool();
            cacheTtlSeconds = ConfigUtil.getInt("redis.song.dim.ttl.seconds", 3600);
        }

        @Override
        public String map(String value) throws Exception {
            JSONObject object = JSON.parseObject(value);
            String songId = object.getString("songId");

            if (songId == null || songId.isEmpty()) {
                return object.toJSONString();
            }

            JSONObject songDim = getSongDim(songId);
            if (songDim != null) {
                object.putAll(songDim);
            }

            return object.toJSONString();
        }

        private JSONObject getSongDim(String songId) throws Exception {
            String cacheKey = RedisUtil.buildSongDimKey(songId);
            String cacheValue = RedisUtil.get(jedisPool, cacheKey);

            if (cacheValue != null && !cacheValue.isEmpty()) {
                return JSON.parseObject(cacheValue);
            }

            JSONObject songDim = querySongDimFromPhoenix(songId);
            if (songDim != null) {
                RedisUtil.setex(jedisPool, cacheKey, cacheTtlSeconds, songDim.toJSONString());
            }
            return songDim;
        }

        private JSONObject querySongDimFromPhoenix(String songId) throws Exception {
            pst.setString(1, songId);
            try (ResultSet rst = pst.executeQuery()) {
                if (rst.next()) {
                    JSONObject songDim = new JSONObject();
                    songDim.put("songName", rst.getString("NAME"));
                    songDim.put("album", rst.getString("ALBUM"));
                    songDim.put("singerInfo", rst.getString("SINGER_INFO"));
                    songDim.put("postTime", rst.getString("POST_TIME"));
                    songDim.put("authorizedCompany", rst.getString("AUTHORIZED_COMPANY"));
                    return songDim;
                }
            }

            logger.warn("未命中歌曲维度，songId={}", songId);
            return null;
        }

        @Override
        public void close() throws Exception {
            if (pst != null) {
                pst.close();
            }
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
            if (jedisPool != null) {
                jedisPool.close();
            }
        }
    }
}
