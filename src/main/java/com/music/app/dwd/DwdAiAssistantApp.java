package com.music.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.music.bean.AiFeedbackRecord;
import com.music.bean.AiQueryRecord;
import com.music.bean.AiResponseRecord;
import com.music.util.ConfigUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * DWD层: AI助手原始事件标准化
 *
 * 当前只保留最薄骨架：
 * 1. 从 ODS topic 读取 AI 原始事件
 * 2. 预留 DWD 三类标准事件与目标 topic
 * 3. 把核心分流 / 映射逻辑留给用户自己实现
 *
 * 需要你后续补的核心点：
 * - ODS 统一外壳校验
 * - 按 eventType 拆分 query / response / feedback
 * - 三类事件的标准字段映射与 Kafka sink
 */
public class DwdAiAssistantApp {

    public static void main(String[] args) throws Exception {
        ConfigUtil.init(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ConfigUtil.getInt("flink.parallelism", 2));
        env.getConfig().disableClosureCleaner();

        String kafkaServers = ConfigUtil.get("kafka.bootstrap.servers");
        String odsTopicName = ConfigUtil.get("kafka.ods.log.ai.assistant");
        String queryTopicName = ConfigUtil.get("kafka.dwd.ai.query");
        String responseTopicName = ConfigUtil.get("kafka.dwd.ai.response");
        String feedbackTopicName = ConfigUtil.get("kafka.dwd.ai.feedback");
        String startingOffsets = ConfigUtil.get("kafka.dwd.ai.starting-offsets", "latest");

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaServers)
            .setTopics(odsTopicName)
            .setGroupId("dwd-ai-assistant-group")
            .setStartingOffsets(resolveOffsetsInitializer(startingOffsets))
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> odsStream = env
            .fromSource(source, WatermarkStrategy.noWatermarks(), "AI Assistant ODS Source")
            .name("AI Assistant ODS Source");

        // TODO 1: 在这里做统一外壳校验，并决定脏数据怎么处理
        // 输入：odsStream
        // 目标：过滤掉不合法的 AI 原始事件
        DataStream<String> invalidStream = odsStream
            .filter(value -> !isValidAiMessage(value))
            .name("AI Assistant Invalid Filter");

        SingleOutputStreamOperator<String> cleanStream = odsStream
            .filter(DwdAiAssistantApp::isValidAiMessage)
            .name("AI Assistant Clean Filter");

        // TODO 2: 在这里按 eventType 拆成三条流
        SingleOutputStreamOperator<String> splitStream = cleanStream
            .process(new AiMessageProcessFunction())
            .name("AI Assistant Split");
        // 目标 topic：
        // - query    -> queryTopicName
        // - response -> responseTopicName
        // - feedback -> feedbackTopicName

        // TODO 3: 在这里把三条流分别映射成标准事件
        DataStream<String> queryStream = splitStream.getSideOutput(AiMessageProcessFunction.QUERY_TAG);
        DataStream<String> responseStream = splitStream.getSideOutput(AiMessageProcessFunction.RESPONSE_TAG);
        DataStream<String> feedbackStream = splitStream.getSideOutput(AiMessageProcessFunction.FEEDBACK_TAG);
        DataStream<String> unknownStream = splitStream.getSideOutput(AiMessageProcessFunction.UNKNOWN_TAG);

        queryStream
            .map(value -> JSON.toJSONString(AiQueryRecord.createQueryRecord(value)))
            .sinkTo(createKafkaSink(kafkaServers, queryTopicName))
            .name("AI Assistant Query");

        responseStream
            .map(value -> JSON.toJSONString(AiResponseRecord.createAiResponseRecord(value)))
            .sinkTo(createKafkaSink(kafkaServers, responseTopicName))
            .name("AI Assistant Response");

        feedbackStream
            .map(value -> JSON.toJSONString(AiFeedbackRecord.createAiFeedbackRecord(value)))
            .sinkTo(createKafkaSink(kafkaServers, feedbackTopicName))
            .name("AI Assistant Feedback");

        invalidStream.print("AI Assistant Invalid");
        unknownStream.print("AI Assistant Unknown");
        // 参考 Bean：
        // - AiQueryRecord
        // - AiResponseRecord
        // - AiFeedbackRecord

        odsStream.print(
            "ods=" + odsTopicName
                + " query=" + queryTopicName
                + " response=" + responseTopicName
                + " feedback=" + feedbackTopicName
        );

        env.execute("DWD AI Assistant App");
    }

    private static boolean isValidAiMessage(String message) {
        try {
            JSONObject jsonObject = JSON.parseObject(message);
            return hasText(jsonObject.getString("eventId"))
                && hasText(jsonObject.getString("eventType"))
                && hasText(jsonObject.getString("eventTime"))
                && hasText(jsonObject.getString("requestId"))
                && hasText(jsonObject.getString("sessionId"))
                && jsonObject.get("userId") != null
                && jsonObject.getJSONObject("payload") != null;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
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

    private static OffsetsInitializer resolveOffsetsInitializer(String offsetMode) {
        if ("latest".equalsIgnoreCase(offsetMode)) {
            return OffsetsInitializer.latest();
        }
        return OffsetsInitializer.earliest();
    }

    private static class AiMessageProcessFunction extends ProcessFunction<String, String> {
        public static final OutputTag<String> QUERY_TAG = new OutputTag<String>("query"){};
        public static final OutputTag<String> RESPONSE_TAG = new OutputTag<String>("response"){};
        public static final OutputTag<String> FEEDBACK_TAG = new OutputTag<String>("feedback"){};
        public static final OutputTag<String> UNKNOWN_TAG = new OutputTag<String>("unknown"){};

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) {
            // 这里只兜底 JSON 解析异常。分流本身如果出错，直接暴露真实异常，便于定位。
            JSONObject jsonObject;
            try {
                jsonObject = JSON.parseObject(value);
            } catch (Exception e) {
                ctx.output(UNKNOWN_TAG, value);
                System.err.println("AI 消息 JSON 解析失败: " + value);
                e.printStackTrace();
                return;
            }

            String eventType = jsonObject.getString("eventType");
            if ("AI_QUERY".equals(eventType)) {
                ctx.output(QUERY_TAG, value);
                return;
            }
            if ("AI_RESPONSE".equals(eventType)) {
                ctx.output(RESPONSE_TAG, value);
                return;
            }
            if ("AI_FEEDBACK".equals(eventType)) {
                ctx.output(FEEDBACK_TAG, value);
                return;
            }

            ctx.output(UNKNOWN_TAG, value);
        }
    }
}
