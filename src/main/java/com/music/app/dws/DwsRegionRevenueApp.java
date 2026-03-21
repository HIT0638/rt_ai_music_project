package com.music.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.music.bean.MachineConsumeWideRecord;
import com.music.bean.RegionRevenueStats;
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

import java.time.*;
import java.time.format.DateTimeFormatter;

public class DwsRegionRevenueApp {

    private static final DateTimeFormatter WINDOW_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // TODO: 你自己定义 action_time 的解析格式
    private static final DateTimeFormatter ACTION_TIME_FORMATTER =
         DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        ConfigUtil.init(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableClosureCleaner();
        env.setParallelism(ConfigUtil.getInt("flink.parallelism", 2));

        String kafkaServers = ConfigUtil.get("kafka.bootstrap.servers");
        String dwmTopicName = ConfigUtil.get(
            "kafka.dwm.machine.consume.wide",
            "dwm_machine_consume_wide_rt");
        String dwsTopicName = ConfigUtil.get(
            "kafka.dws.region.revenue",
            "dws_region_revenue_rt");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaServers)
            .setTopics(dwmTopicName)
            .setGroupId("dws-region-revenue-group")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<MachineConsumeWideRecord> wideRecordStream = env
            .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .name("DWM Machine Consume Wide Source")
            .map(DwsRegionRevenueApp::parseWideRecord)
            .name("Parse Region Revenue Wide Record")
            .filter(DwsRegionRevenueApp::isValidRecord)
            .name("Filter Valid Revenue Record")
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<MachineConsumeWideRecord>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((event, ts) -> {
                        // TODO: 你自己把 actionTime 解析成事件时间毫秒值
                        LocalDateTime localTime = LocalDateTime.parse(event.getActionTime(), ACTION_TIME_FORMATTER);
                        long milliSeconds = localTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                        return milliSeconds;
                    })
            )
            .name("Assign Watermarks");

        DataStream<RegionRevenueStats> statsStream = wideRecordStream
            .keyBy(DwsRegionRevenueApp::buildRegionKey)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .process(new RegionRevenueWindowProcessFunction())
            .name("Region Revenue Aggregate");

        statsStream
            .map(JSON::toJSONString)
            .sinkTo(createKafkaSink(kafkaServers, dwsTopicName))
            .name("DWS Region Revenue Sink");

        env.execute("DWS Region Revenue App");
    }

    private static MachineConsumeWideRecord parseWideRecord(String value) {
        JSONObject object = JSON.parseObject(value);
        MachineConsumeWideRecord record = new MachineConsumeWideRecord();
        record.setId(object.getLong("id"));
        record.setMid(object.getLong("mid"));
        record.setUid(object.getLong("uid"));
        record.setAmount(object.getLong("amount"));
        record.setPkgId(object.getInteger("pkgId"));
        record.setPkgName(object.getString("pkgName"));
        record.setActivityId(object.getInteger("activityId"));
        record.setActivityName(object.getString("activityName"));
        record.setActionTime(object.getString("actionTime"));
        record.setBillDate(object.getString("billDate"));
        record.setProvince(object.getString("province"));
        record.setCity(object.getString("city"));
        return record;
    }

    private static boolean isValidRecord(MachineConsumeWideRecord value) {
        return value != null
            && value.getAmount() != null
            && value.getActionTime() != null
            && value.getProvince() != null
            && value.getCity() != null;
    }

    private static String buildRegionKey(MachineConsumeWideRecord value) {
        // TODO: 你自己决定第一版按 province+city 还是只按 city
        return value.getProvince() + "_" + value.getCity();
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

    private static class RegionRevenueWindowProcessFunction
        extends ProcessWindowFunction<MachineConsumeWideRecord, RegionRevenueStats, String, TimeWindow> {

        @Override
        public void process(
            String regionKey,
            Context context,
            Iterable<MachineConsumeWideRecord> elements,
            Collector<RegionRevenueStats> out) {

            RegionRevenueStats ret = new RegionRevenueStats();
            long orderCount = 0L;
            long totalAmount = 0L;

            boolean isFirst = true;
            for (MachineConsumeWideRecord record : elements) {
                orderCount ++;
                totalAmount += record.getAmount();
                if (isFirst) {
                    ret.setProvince(record.getProvince());
                    ret.setCity(record.getCity());
                    isFirst = false;
                }
            }

            ret.setOrderCount(orderCount);
            ret.setTotalAmount(totalAmount);
            ret.setWindowStart(formatWindowTime(context.window().getStart()));
            ret.setWindowEnd(formatWindowTime(context.window().getEnd()));
            // TODO 1: 统计窗口内 orderCount
            // TODO 2: 统计窗口内 totalAmount
            // TODO 3: 取窗口内第一条记录回填 province/city
            // TODO 4: 构造 RegionRevenueStats
            // TODO 5: 设置 windowStart/windowEnd
            // TODO 6: out.collect(stats)
            out.collect(ret);
        }

        private String formatWindowTime(long timestamp) {
            return LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp),
                ZoneId.systemDefault()
            ).format(WINDOW_FORMATTER);
        }
    }
}
