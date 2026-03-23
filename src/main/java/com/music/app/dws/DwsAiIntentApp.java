package com.music.app.dws;

import com.alibaba.fastjson.JSON;
import com.music.bean.AiFeedbackRecord;
import com.music.bean.AiIntentStats;
import com.music.bean.AiResponseWideRecord;
import com.music.util.ConfigUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
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

public class DwsAiIntentApp {

    private static final DateTimeFormatter TIME_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // TODO: 由你决定第一版 unmatched feedback 等多久再清理更合适。
    private static final long PENDING_FEEDBACK_WAIT_MS = Duration.ofMinutes(60).toMillis();

    public static void main(String[] args) throws Exception {
        ConfigUtil.init(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableClosureCleaner();
        env.setParallelism(ConfigUtil.getInt("flink.parallelism", 2));

        String kafkaServers = ConfigUtil.get("kafka.bootstrap.servers");
        String responseTopicName = ConfigUtil.get("kafka.dwm.ai.response.wide", "dwm_ai_response_wide_rt");
        String feedbackTopicName = ConfigUtil.get("kafka.dwd.ai.feedback", "dwd_ai_feedback_rt");
        String dwsTopicName = ConfigUtil.get("kafka.dws.ai.intent", "dws_ai_intent_rt");

        KafkaSource<String> responseSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaServers)
            .setTopics(responseTopicName)
            .setGroupId("dws-ai-intent-response-group")
            .setStartingOffsets(resolveOffsetsInitializer())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        KafkaSource<String> feedbackSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaServers)
            .setTopics(feedbackTopicName)
            .setGroupId("dws-ai-intent-feedback-group")
            .setStartingOffsets(resolveOffsetsInitializer())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<AiResponseWideRecord> responseStream = env
            .fromSource(responseSource, WatermarkStrategy.noWatermarks(), "AI Response Wide Source")
            .name("AI Response Wide Source")
            .map(value -> JSON.parseObject(value, AiResponseWideRecord.class))
            .name("Parse AI Response Wide Record")
            .filter(DwsAiIntentApp::isValidResponseRecord)
            .name("Filter Valid AI Response Wide");

        DataStream<AiFeedbackRecord> feedbackStream = env
            .fromSource(feedbackSource, WatermarkStrategy.noWatermarks(), "AI Feedback Source")
            .name("AI Feedback Source")
            .map(value -> JSON.parseObject(value, AiFeedbackRecord.class))
            .name("Parse AI Feedback Record")
            .filter(DwsAiIntentApp::isValidFeedbackRecord)
            .name("Filter Valid AI Feedback");

        SingleOutputStreamOperator<AiIntentMetricEvent> metricStream = responseStream
                .keyBy(AiResponseWideRecord::getRequestId)
                .connect(feedbackStream.keyBy(AiFeedbackRecord::getRequestId))
                .process(new IntentMetricProcessFunction())
                .name("Join Response And Feedback By RequestId");



        DataStream<AiIntentStats> statsStream = metricStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<AiIntentMetricEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withIdleness(Duration.ofSeconds(15))
                    .withTimestampAssigner((event, ts) -> event.eventTimeMillis)
            )
            .name("Assign Intent Metric Watermarks")
            .keyBy(new KeySelector<AiIntentMetricEvent, Tuple2<String, String>>() {
                @Override
                public Tuple2<String, String> getKey(AiIntentMetricEvent aiIntentMetricEvent) throws Exception {
                    return Tuple2.of(aiIntentMetricEvent.intentType, aiIntentMetricEvent.intentGroup);
                }
            })
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .aggregate(new AggregateIntentFunction(), new IntentWindowProcessFunction())
            .name("AI Intent Aggregate");

        statsStream
            .map(JSON::toJSONString)
            .sinkTo(createKafkaSink(kafkaServers, dwsTopicName))
            .name("DWS AI Intent Sink");

        env.execute("DWS AI Intent App");
    }

    private static boolean isValidResponseRecord(AiResponseWideRecord value) {
        return value != null
            && value.getRequestId() != null
            && value.getResponseTime() != null
            && value.getIntentType() != null
            && value.getIntentGroup() != null;
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

    private static AiIntentMetricEvent createResponseIntentMetric(AiResponseWideRecord value) {
        AiIntentMetricEvent event = new AiIntentMetricEvent();
        event.eventTimeMillis = parseTime(value.getResponseTime());
        event.intentType = value.getIntentType();
        event.intentGroup = value.getIntentGroup();
        event.requestCount = 1L;
        event.successCount = Boolean.TRUE.equals(value.getSuccess()) ? 1L : 0L;
        event.latencySum = value.getLatencyMs() == null ? 0L : value.getLatencyMs();
        event.latencyCount = value.getLatencyMs() == null ? 0L : 1L;
        return event;
    }

    private static AiIntentMetricEvent createFeedbackIntentMetric(
        Tuple2<String, String> intentRef,
        String feedbackType,
        long feedbackTimeMillis) {

        AiIntentMetricEvent event = new AiIntentMetricEvent();
        event.eventTimeMillis = feedbackTimeMillis;
        event.intentType = intentRef.f0;
        event.intentGroup = intentRef.f1;

        String normalizedFeedbackType = feedbackType.trim().toUpperCase();
        switch (normalizedFeedbackType) {
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

    public static class AiIntentMetricEvent {
        public long eventTimeMillis;
        public String intentType;
        public String intentGroup;
        public long requestCount;
        public long successCount;
        public long latencySum;
        public long latencyCount;
        public long likeCount;
        public long dislikeCount;
        public long followupCount;
        public long clickCount;
    }

    public static class AiIntentAcc {
        public long requestCount;
        public long successCount;
        public long latencySum;
        public long latencyCount;
        public long likeCount;
        public long dislikeCount;
        public long followupCount;
        public long clickCount;
    }

    private static final class IntentMetricProcessFunction
        extends KeyedCoProcessFunction<String, AiResponseWideRecord, AiFeedbackRecord, AiIntentMetricEvent> {

        private transient ValueState<Tuple2<String, String>> responseIntentState;
        private transient ListState<Tuple3<String, Long, String>> pendingFeedbackState;
        private transient ValueState<Long> cleanupTimerState;

        @Override
        public void open(Configuration parameters) {
            responseIntentState = getRuntimeContext().getState(
                new ValueStateDescriptor<>(
                    "response-intent-state",
                    Types.TUPLE(Types.STRING, Types.STRING)
                )
            );
            pendingFeedbackState = getRuntimeContext().getListState(
                new ListStateDescriptor<>(
                    "pending-feedback-state",
                    Types.TUPLE(Types.STRING, Types.LONG, Types.STRING)
                )
            );
            cleanupTimerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pending-feedback-cleanup-timer", Long.class)
            );
        }

        @Override
        public void processElement1(
            AiResponseWideRecord value,
            Context ctx,
            Collector<AiIntentMetricEvent> out) throws Exception {

            Tuple2<String, String> intentRef = Tuple2.of(value.getIntentType(), value.getIntentGroup());

            out.collect(createResponseIntentMetric(value));
            responseIntentState.update(intentRef);

            for (Tuple3<String, Long, String> pending : pendingFeedbackState.get()) {
                out.collect(createFeedbackIntentMetric(intentRef, pending.f0, pending.f1));
            }

            pendingFeedbackState.clear();

            Long cleanupTimer = cleanupTimerState.value();
            if (cleanupTimer != null) {
                ctx.timerService().deleteProcessingTimeTimer(cleanupTimer);
                cleanupTimerState.clear();
            }
        }

        @Override
        public void processElement2(
            AiFeedbackRecord value,
            Context ctx,
            Collector<AiIntentMetricEvent> out) throws Exception {

            Tuple2<String, String> intentRef = responseIntentState.value();
            long feedbackTimeMillis = parseTime(value.getFeedbackTime());

            if (intentRef != null) {
                out.collect(createFeedbackIntentMetric(intentRef, value.getFeedbackType(), feedbackTimeMillis));
                return;
            }

            pendingFeedbackState.add(Tuple3.of(value.getFeedbackType(), feedbackTimeMillis, value.getEventId()));

            Long cleanupTimer = cleanupTimerState.value();
            if (cleanupTimer == null) {
                long triggerTime = ctx.timerService().currentProcessingTime() + PENDING_FEEDBACK_WAIT_MS;
                ctx.timerService().registerProcessingTimeTimer(triggerTime);
                cleanupTimerState.update(triggerTime);
            }
        }

        @Override
        public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<AiIntentMetricEvent> out) throws Exception {

            // TODO: 由你决定 unmatched feedback 最终是写 side output、打日志，还是直接丢弃计数。
            // 直接丢弃即可
            pendingFeedbackState.clear();
            cleanupTimerState.clear();
        }
    }

    private static final class AggregateIntentFunction
        implements AggregateFunction<AiIntentMetricEvent, AiIntentAcc, AiIntentAcc> {

        @Override
        public AiIntentAcc createAccumulator() {
            return new AiIntentAcc();
        }

        @Override
        public AiIntentAcc add(AiIntentMetricEvent value, AiIntentAcc acc) {
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
        public AiIntentAcc getResult(AiIntentAcc acc) {
            return acc;
        }

        @Override
        public AiIntentAcc merge(AiIntentAcc a, AiIntentAcc b) {
            AiIntentAcc merged = new AiIntentAcc();
            merged.requestCount = a.requestCount + b.requestCount;
            merged.successCount = a.successCount + b.successCount;
            merged.latencySum = a.latencySum + b.latencySum;
            merged.latencyCount = a.latencyCount + b.latencyCount;
            merged.likeCount = a.likeCount + b.likeCount;
            merged.dislikeCount = a.dislikeCount + b.dislikeCount;
            merged.followupCount = a.followupCount + b.followupCount;
            merged.clickCount = a.clickCount + b.clickCount;
            return merged;
        }
    }

    private static final class IntentWindowProcessFunction
        extends ProcessWindowFunction<AiIntentAcc, AiIntentStats, Tuple2<String, String>, TimeWindow> {

        @Override
        public void process(
            Tuple2<String, String> key,
            Context context,
            Iterable<AiIntentAcc> elements,
            Collector<AiIntentStats> out) {

            AiIntentAcc acc = elements.iterator().next();
            AiIntentStats stats = new AiIntentStats();
            stats.setWindowStart(formatWindowTime(context.window().getStart()));
            stats.setWindowEnd(formatWindowTime(context.window().getEnd()));
            stats.setIntentType(key.f0);
            stats.setIntentGroup(key.f1);
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

    private static long parseTime(String value) {
        return LocalDateTime.parse(value, TIME_FORMATTER)
            .atZone(ZoneId.systemDefault())
            .toInstant()
            .toEpochMilli();
    }
}
