package com.music.app.dwm;

import com.alibaba.fastjson.JSON;
import com.music.bean.AiResponseRecord;
import com.music.bean.AiResponseWideRecord;
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

public class DwmAiResponseWideApp {

    public static void main(String[] args) throws Exception {
        ConfigUtil.init(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableClosureCleaner();
        env.setParallelism(ConfigUtil.getInt("flink.parallelism", 2));

        String kafkaServers = ConfigUtil.get("kafka.bootstrap.servers");
        String dwdTopicName = ConfigUtil.get("kafka.dwd.ai.response", "dwd_ai_response_rt");
        String dwmTopicName = ConfigUtil.get("kafka.dwm.ai.response.wide", "dwm_ai_response_wide_rt");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaServers)
            .setTopics(dwdTopicName)
            .setGroupId("dwm-ai-response-wide-group")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<AiResponseRecord> responseStream = env
            .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .name("DWD AI Response Source")
            .map(value -> JSON.parseObject(value, AiResponseRecord.class))
            .name("Parse AI Response Record")
            .filter(DwmAiResponseWideApp::isValidResponseRecord)
            .name("Filter Valid AI Response");

        DataStream<AiResponseWideRecord> wideStream = responseStream
            .map(DwmAiResponseWideApp::buildWideRecord)
            .name("Build AI Response Wide Record");

        wideStream
            .map(JSON::toJSONString)
            .sinkTo(createKafkaSink(kafkaServers, dwmTopicName))
            .name("DWM AI Response Wide Sink");

        env.execute("DWM AI Response Wide App");
    }

    private static AiResponseWideRecord buildWideRecord(AiResponseRecord responseRecord) {
        AiResponseWideRecord wideRecord = AiResponseWideRecord.createWideRecord(responseRecord);

        // TODO 1: 如果后面要分析推荐命中歌曲/歌手的业务属性，可以在这里补维度查询或缓存 enrich。
        // TODO 2: 如果第一版需要补用户基础字段，也在这里扩展，不要在 DWD 再回写用户维度。
        // TODO 3: 当前先保留轻量分析友好字段：结果列表、结果数、首个命中对象、responseSource 等。

        return wideRecord;
    }

    private static boolean isValidResponseRecord(AiResponseRecord value) {
        return value != null
            && value.getRequestId() != null
            && value.getSessionId() != null
            && value.getEventTime() != null
            && value.getIntentType() != null
            && value.getResponseSource() != null;
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
}
