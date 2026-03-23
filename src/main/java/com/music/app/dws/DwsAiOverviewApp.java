package com.music.app.dws;

import com.alibaba.fastjson.JSON;
import com.music.bean.AiFeedbackRecord;
import com.music.bean.AiOverviewStats;
import com.music.bean.AiResponseWideRecord;
import com.music.util.ConfigUtil;
import org.apache.flink.api.common.functions.AggregateFunction;
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

public class DwsAiOverviewApp {

    private static final DateTimeFormatter TIME_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        ConfigUtil.init(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableClosureCleaner();
        env.setParallelism(ConfigUtil.getInt("flink.parallelism", 2));

        String kafkaServers = ConfigUtil.get("kafka.bootstrap.servers");
        String responseTopicName = ConfigUtil.get("kafka.dwm.ai.response.wide", "dwm_ai_response_wide_rt");
        String feedbackTopicName = ConfigUtil.get("kafka.dwd.ai.feedback", "dwd_ai_feedback_rt");
        String dwsTopicName = ConfigUtil.get("kafka.dws.ai.overview", "dws_ai_overview_rt");

        KafkaSource<String> responseSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaServers)
            .setTopics(responseTopicName)
            .setGroupId("dws-ai-overview-response-group")
            .setStartingOffsets(resolveOffsetsInitializer())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        KafkaSource<String> feedbackSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaServers)
            .setTopics(feedbackTopicName)
            .setGroupId("dws-ai-overview-feedback-group")
            .setStartingOffsets(resolveOffsetsInitializer())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<AiResponseWideRecord> responseStream = env
            .fromSource(responseSource, WatermarkStrategy.noWatermarks(), "AI Response Wide Source")
            .name("AI Response Wide Source")
            .map(value -> JSON.parseObject(value, AiResponseWideRecord.class))
            .name("Parse AI Response Wide Record")
            .filter(DwsAiOverviewApp::isValidResponseRecord)
            .name("Filter Valid AI Response Wide");

        DataStream<AiFeedbackRecord> feedbackStream = env
            .fromSource(feedbackSource, WatermarkStrategy.noWatermarks(), "AI Feedback Source")
            .name("AI Feedback Source")
            .map(value -> JSON.parseObject(value, AiFeedbackRecord.class))
            .name("Parse AI Feedback Record")
            .filter(DwsAiOverviewApp::isValidFeedbackRecord)
            .name("Filter Valid AI Feedback");

        // 第一版当前口径 1：overview 的公共指标事件模型。
        // 业务目标：
        // - overview 只看整体总览，不按 intent/song/singer 拆。
        // - 所以这里使用“最小够用”的指标增量事件，而不是继续拉宽业务字段。
        // 当前字段：
        // - eventTimeMillis
        // - requestCount / successCount
        // - latencySum / latencyCount
        // - likeCount / dislikeCount / followupCount / clickCount
        DataStream<AiOverviewMetricEvent> responseMetricStream = responseStream
                .map(DwsAiOverviewApp::createAiResponseMetricEvent)
                .name("Ai Response Metric Event Create");

        DataStream<AiOverviewMetricEvent> feedbackMetricStream = feedbackStream
                .map(DwsAiOverviewApp::createAiFeedbackMetricEvent)
                .name("Ai Feedbac Metric Event Create");

        // 第一版当前口径 2：overview 的事件时间口径。
        // - response 按 responseTime 入窗
        // - feedback 按 feedbackTime 入窗
        // 这里的时间口径决定“哪些事件会落入同一个 10 秒窗口”。
        DataStream<AiOverviewStats> statsStream = responseMetricStream
                .union(feedbackMetricStream)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<AiOverviewMetricEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, ts) -> {
                                    return event.eventTimeMillis;
                                })
                )
                .keyBy(value -> "all")
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateOverviewFunction(), new AggregateWindowProcessFunction());

        // 第一版当前口径 3：response / feedback 先分别转换成统一的指标增量事件，再 union。
        // response 当前贡献：
        // - request_count：第一版先按 response 条数近似
        // - success_count
        // - latency
        // feedback 当前贡献：
        // - like / dislike / followup / click
        // 注意：
        // - 这里不是 connect
        // - 也不是把 AiResponseWideRecord 和 AiFeedbackRecord 整体塞进结果表

        // 第一版当前口径 4：对 union 后的指标增量事件做窗口聚合。
        // - assignTimestampsAndWatermarks(...)
        // - keyBy(value -> "all")
        // - 10 秒滚动窗口
        // - AggregateFunction 做增量累加
        // - ProcessWindowFunction 补 windowStart / windowEnd，输出 AiOverviewStats

        // 第一版当前口径 5：把 AiOverviewStats 写入 dws_ai_overview_rt。
        statsStream
                .map(JSON::toJSONString)
                .sinkTo(createKafkaSink(kafkaServers, dwsTopicName));

        env.execute();
    }

    private static boolean isValidResponseRecord(AiResponseWideRecord value) {
        return value != null
            && value.getRequestId() != null
            && value.getResponseTime() != null;
    }

    private static boolean isValidFeedbackRecord(AiFeedbackRecord value) {
        return value != null
            && value.getRequestId() != null
            && value.getFeedbackTime() != null
            && value.getFeedbackType() != null;
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

    private static OffsetsInitializer resolveOffsetsInitializer() {
        String offsetMode = ConfigUtil.get("kafka.dws.ai.starting-offsets", "earliest");
        if ("latest".equalsIgnoreCase(offsetMode)) {
            return OffsetsInitializer.latest();
        }
        return OffsetsInitializer.earliest();
    }

    private static long parseTime(String value) {
        return LocalDateTime.parse(value, TIME_FORMATTER)
            .atZone(ZoneId.systemDefault())
            .toInstant()
            .toEpochMilli();
    }

    public static class AiOverviewMetricEvent {
        // 第一版当前口径：
        // - eventTimeMillis
        // - requestCount / successCount
        // - latencySum / latencyCount
        // - likeCount / dislikeCount / followupCount / clickCount
        public long eventTimeMillis;
        public long requestCount;
        public long successCount;
        public long latencySum;
        public long latencyCount;
        public long likeCount;
        public long dislikeCount;
        public long followupCount;
        public long clickCount;
    }

    private static AiOverviewMetricEvent createAiResponseMetricEvent(AiResponseWideRecord value){
        AiOverviewMetricEvent event = new AiOverviewMetricEvent();
        event.eventTimeMillis = parseTime(value.getResponseTime());
        event.requestCount = 1L;
        event.successCount = Boolean.TRUE.equals(value.getSuccess()) ? 1L : 0L;
        event.latencySum = value.getLatencyMs() == null ? 0L : value.getLatencyMs();
        event.latencyCount = value.getLatencyMs() == null ? 0L : 1L;
        return event;
    }

    private static AiOverviewMetricEvent createAiFeedbackMetricEvent(AiFeedbackRecord value){
        AiOverviewMetricEvent event = new AiOverviewMetricEvent();
        event.eventTimeMillis = parseTime(value.getFeedbackTime());

        String feedbackType = value.getFeedbackType().trim().toUpperCase();
        switch (feedbackType) {
            case "LIKE":
                event.likeCount = 1L;
                break;
            case "DISLIKE":
                event.dislikeCount = 1L;
                break;
            case "FOLLOWUP":
                event.followupCount = 1L;
                break;
            case "CLICK":
                event.clickCount = 1L;
                break;
            default:
                break;
        }
        return event;
    }

    public static class AiOverviewAcc {
        public long requestCount;
        public long successCount;
        public long latencySum;
        public long latencyCount;
        public long likeCount;
        public long dislikeCount;
        public long followupCount;
        public long clickCount;
    }

    private static class AggregateOverviewFunction
        implements AggregateFunction<AiOverviewMetricEvent, AiOverviewAcc, AiOverviewAcc> {

        @Override
        public AiOverviewAcc createAccumulator() {
            return new AiOverviewAcc();
        }

        @Override
        public AiOverviewAcc add(AiOverviewMetricEvent value, AiOverviewAcc acc) {
            // 第一版当前口径：
            // - 所有 metric event 在同一个窗口 accumulator 里累计
            // - response 贡献 request/success/latency
            // - feedback 贡献 like/dislike/followup/click
            acc.requestCount += value.requestCount;
            acc.successCount += value.successCount;
            acc.latencySum += value.latencySum;
            acc.latencyCount += value.latencyCount;
            acc.likeCount += value.likeCount;
            acc.dislikeCount += value.dislikeCount;
            acc.followupCount += value.followupCount;
            acc.clickCount += value.clickCount;

            return acc;
        }

        @Override
        public AiOverviewAcc getResult(AiOverviewAcc acc) {
            return acc;
        }

        @Override
        public AiOverviewAcc merge(AiOverviewAcc a, AiOverviewAcc b) {
            AiOverviewAcc merged = new AiOverviewAcc();
            // 第一版当前实现：
            // - tumbling window 下 merge 通常不是重点
            // - 这里先机械合并两个 acc，满足 AggregateFunction 接口要求
            merged.likeCount = a.likeCount + b.likeCount;
            merged.dislikeCount = a.dislikeCount + b.dislikeCount;
            merged.followupCount = a.followupCount + b.followupCount;
            merged.clickCount = a.clickCount + b.clickCount;
            merged.requestCount = a.requestCount + b.requestCount;
            merged.successCount = a.successCount + b.successCount;
            merged.latencySum = a.latencySum + b.latencySum;
            merged.latencyCount = a.latencyCount + b.latencyCount;

            return merged;
        }
    }

    private static class AggregateWindowProcessFunction
        extends ProcessWindowFunction<AiOverviewAcc, AiOverviewStats, String, TimeWindow> {

        @Override
        public void process(
            String key,
            Context context,
            Iterable<AiOverviewAcc> elements,
            Collector<AiOverviewStats> out) {

            AiOverviewAcc acc = elements.iterator().next();
            AiOverviewStats stats = new AiOverviewStats();

            // 第一版当前实现：
            // - 把窗口 accumulator 转成最终输出对象 AiOverviewStats
            // - avgLatencyMs = latencySum / latencyCount
            // - windowStart / windowEnd 由 ProcessWindowFunction 补齐
            stats.setWindowStart(formatWindowTime(context.window().getStart()));
            stats.setWindowEnd(formatWindowTime(context.window().getEnd()));
            stats.setRequestCount(acc.requestCount);
            stats.setSuccessCount(acc.successCount);
            stats.setAvgLatencyMs(acc.latencyCount == 0 ? 0L : acc.latencySum / acc.latencyCount);
            stats.setLikeCount(acc.likeCount);
            stats.setDislikeCount(acc.dislikeCount);
            stats.setFollowupCount(acc.followupCount);
            stats.setClickCount(acc.clickCount);
            out.collect(stats);
            
        }

        private String formatWindowTime(long timestamp) {
            return LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(timestamp),
                    ZoneId.systemDefault()
            ).format(TIME_FORMATTER);
        }
    }

}
