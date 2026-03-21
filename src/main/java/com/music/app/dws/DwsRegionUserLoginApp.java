package com.music.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.music.bean.RegionLoginStats;
import com.music.bean.UserLoginLocationWideRecord;
import com.music.util.ConfigUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
import java.util.HashSet;

public class DwsRegionUserLoginApp {

    private static final DateTimeFormatter WINDOW_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        ConfigUtil.init(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ConfigUtil.getInt("flink.parallelism", 2));

        String kafkaServers = ConfigUtil.get("kafka.bootstrap.servers");
        String dwmTopicName = ConfigUtil.get(
            "kafka.dwm.user.login.location.wide",
            "dwm_user_login_location_wide_rt");
        String dwsTopicName = ConfigUtil.get(
            "kafka.dws.region.user.login",
            "dws_region_user_login_rt");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaServers)
            .setTopics(dwmTopicName)
            .setGroupId("dws-region-user-login-group")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<UserLoginLocationWideRecord> wideRecordStream = env
            .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .name("DWM User Login Location Wide Source")
            .map(DwsRegionUserLoginApp::parseWideRecord)
            .name("Parse Region Login Wide Record")
            .filter(DwsRegionUserLoginApp::isValidRecord)
            .name("Filter Valid Region Login Record")
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<UserLoginLocationWideRecord>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((event, ts) -> event.getLoginDt())
            )
            .name("Assign Watermarks");

        DataStream<RegionLoginStats> statsStream = wideRecordStream
            .keyBy(DwsRegionUserLoginApp::buildRegionKey)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .process(new RegionLoginWindowProcessFunction())
            .name("Region User Login Aggregate");

        statsStream
            .map(JSON::toJSONString)
            .sinkTo(createKafkaSink(kafkaServers, dwsTopicName))
            .name("DWS Region User Login Sink");

        env.execute("DWS Region User Login App");
    }

    private static UserLoginLocationWideRecord parseWideRecord(String value) {
        JSONObject object = JSON.parseObject(value);
        UserLoginLocationWideRecord record = new UserLoginLocationWideRecord();
        record.setUserId(object.getLong("userId"));
        record.setLng(object.getString("lng"));
        record.setLat(object.getString("lat"));
        record.setLoginDt(object.getLong("loginDt"));
        record.setMachineId(object.getLong("machineId"));
        record.setProvince(object.getString("province"));
        record.setCity(object.getString("city"));
        record.setDistrict(object.getString("district"));
        record.setAdcode(object.getString("adcode"));
        record.setDate(object.getString("date"));
        record.setHour(object.getString("hour"));
        return record;
    }

    private static boolean isValidRecord(UserLoginLocationWideRecord value) {
        return value != null
            && value.getUserId() != null
            && value.getLoginDt() != null
            && value.getProvince() != null
            && value.getCity() != null
            && value.getDistrict() != null;
    }

    private static String buildRegionKey(UserLoginLocationWideRecord value) {
        // TODO: 如果后面想把统计粒度改成 city 或 province，这里统一调整 key 口径即可。
        return value.getProvince() + "_" + value.getCity() + "_" + value.getDistrict();
    }

    private static KafkaSink<String> createKafkaSink(String bootstrapServers, String topic) {
        return KafkaSink.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(topic)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
    }

    private static class RegionLoginWindowProcessFunction
        extends ProcessWindowFunction<UserLoginLocationWideRecord, RegionLoginStats, String, TimeWindow> {

        @Override
        public void process(
            String regionKey,
            Context context,
            Iterable<UserLoginLocationWideRecord> elements,
            Collector<RegionLoginStats> out) {

            // TODO 1: 在窗口内按 userId 做去重统计，建议使用 HashSet<Long>
            HashSet<Long> userIds = new HashSet<>();

            boolean isFirst = true;
            UserLoginLocationWideRecord first = null;

            for (UserLoginLocationWideRecord record : elements) {
                userIds.add(record.getUserId());
                if (isFirst) {
                    first = record;
                    isFirst = false;
                }
            }

            RegionLoginStats baseStats = createBaseStats(first, context.window());
            baseStats.setLoginUserCount((long) userIds.size());
            out.collect(baseStats);

            // TODO 2: 取窗口内第一条记录，回填 province/city/district/adcode
            // TODO 3: 把 distinct 后的用户数写入 loginUserCount
            // TODO 4: 设置 windowStart / windowEnd
            // TODO 5: 构造 RegionLoginStats 后 out.collect(stats)
        }

        private RegionLoginStats createBaseStats(
            UserLoginLocationWideRecord first,
            TimeWindow window) {

            RegionLoginStats stats = new RegionLoginStats();
            stats.setProvince(first.getProvince());
            stats.setCity(first.getCity());
            stats.setDistrict(first.getDistrict());
            stats.setAdcode(first.getAdcode());
            stats.setWindowStart(formatWindowTime(window.getStart()));
            stats.setWindowEnd(formatWindowTime(window.getEnd()));
            return stats;
        }

        private String formatWindowTime(long timestamp) {
            return LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp),
                ZoneId.systemDefault()
            ).format(WINDOW_FORMATTER);
        }
    }
}
