package com.music.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.music.bean.SingerHotStats;
import com.music.bean.SongPlayWideRecord;
import com.music.util.ConfigUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DwsSingerHotApp {

    private static final DateTimeFormatter WINDOW_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        ConfigUtil.init(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ConfigUtil.getInt("flink.parallelism", 2));

        String kafkaServers = ConfigUtil.get("kafka.bootstrap.servers");
        String dwmTopicName = ConfigUtil.get("kafka.dwm.song.play.wide", "dwm_song_play_wide_rt");
        String dwsTopicName = ConfigUtil.get("kafka.dws.singer.hot", "dws_singer_hot_rt");
        String groupId = "dws-singer-hot-group";

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaServers)
            .setTopics(dwmTopicName)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<SongPlayWideRecord> wideRecordStream = env
            .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .name("DWM Song Play Wide Source")
            .map(DwsSingerHotApp::parseWideRecord)
            .name("Parse Singer Play Wide Record")
            .filter(value -> value.getTimestamp() != null)
            .name("Filter Valid Timestamp")
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<SongPlayWideRecord>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((event, ts) -> event.getTimestamp())
            )
            .name("Assign Watermarks")
            .filter(value -> "play".equals(value.getAction()))
            .filter(value -> value.getSingerInfo() != null && !value.getSingerInfo().isEmpty())
            .name("Filter Valid Singer Play");

        DataStream<SingerHotStats> statsStream = wideRecordStream
            .keyBy(SongPlayWideRecord::getSingerInfo)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .aggregate(new SingerPlayCountAggregate(), new SingerHotWindowProcessFunction())
            .name("Singer Hot Aggregate");

        statsStream
            .map(JSON::toJSONString)
            .sinkTo(createKafkaSink(kafkaServers, dwsTopicName))
            .name("DWS Singer Hot Sink");

        env.execute("DWS Singer Hot App");
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

    private static SongPlayWideRecord parseWideRecord(String value) {
        JSONObject object = JSON.parseObject(value);
        SongPlayWideRecord record = new SongPlayWideRecord();
        record.setSongId(object.getString("songId"));
        record.setSongName(object.getString("songName"));
        record.setSingerInfo(object.getString("singerInfo"));
        record.setAction(object.getString("action"));
        record.setTimestamp(object.getLong("timestamp"));
        return record;
    }

    private static class SingerPlayCountAggregate
        implements AggregateFunction<SongPlayWideRecord, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(SongPlayWideRecord value, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    private static class SingerHotWindowProcessFunction
        extends ProcessWindowFunction<Long, SingerHotStats, String, TimeWindow> {

        @Override
        public void process(
            String singerInfo,
            Context context,
            Iterable<Long> elements,
            Collector<SingerHotStats> out) {

            long playCount = elements.iterator().next();

            SingerHotStats stats = new SingerHotStats();
            stats.setSingerInfo(singerInfo);
            stats.setPlayCount(playCount);
            stats.setWindowStart(formatWindowTime(context.window().getStart()));
            stats.setWindowEnd(formatWindowTime(context.window().getEnd()));

            out.collect(stats);
        }

        private String formatWindowTime(long timestamp) {
            return LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp),
                ZoneId.systemDefault()
            ).format(WINDOW_FORMATTER);
        }
    }
}
