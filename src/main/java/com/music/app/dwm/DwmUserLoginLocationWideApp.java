package com.music.app.dwm;

import com.alibaba.fastjson.JSON;
import com.music.bean.UserLoginLocationInfo;
import com.music.bean.UserLoginLocationWideRecord;
import com.music.function.UserLoginLocationBatchEnrichFunction;
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

public class DwmUserLoginLocationWideApp {

    public static void main(String[] args) throws Exception {
        ConfigUtil.init(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableClosureCleaner();
        env.setParallelism(ConfigUtil.getInt("flink.parallelism", 2));

        String kafkaServers = ConfigUtil.get("kafka.bootstrap.servers");
        String dwdTopicName = ConfigUtil.get(
            "kafka.dwd.user.login.location",
            "dwd_user_login_location_info_rt");
        String dwmTopicName = ConfigUtil.get(
            "kafka.dwm.user.login.location.wide",
            "dwm_user_login_location_wide_rt");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaServers)
            .setTopics(dwdTopicName)
            .setGroupId("dwm-user-login-location-group")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<UserLoginLocationInfo> sourceStream = env
            .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .name("DWD User Login Location Source")
            .map(value -> JSON.parseObject(value, UserLoginLocationInfo.class))
            .name("Parse User Login Location Info")
            .filter(DwmUserLoginLocationWideApp::isValidRecord)
            .name("Filter Valid Login Location");

        DataStream<UserLoginLocationWideRecord> wideStream = sourceStream
            .flatMap(new UserLoginLocationBatchEnrichFunction(
                ConfigUtil.getInt("amap.batch.size", 10),
                ConfigUtil.get("amap.web.api.key", "")
            ))
            .name("Batch Enrich With AMap");

        wideStream
            .map(JSON::toJSONString)
            .sinkTo(createKafkaSink(kafkaServers, dwmTopicName))
            .name("DWM User Login Location Wide Sink");

        env.execute("DWM User Login Location Wide App");
    }

    private static boolean isValidRecord(UserLoginLocationInfo value) {
        return value != null
            && value.getUserId() != null
            && value.getLng() != null
            && value.getLat() != null
            && value.getLoginDt() != null;
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
