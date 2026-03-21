package com.music.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * 动态路由序列化
 * 根据数据中的 sink_topic 字段决定写入哪个 Topic
 */
public class DynamicKafkaSerializationSchema
    implements KafkaRecordSerializationSchema<String> {

    private static final Logger logger = LoggerFactory.getLogger(DynamicKafkaSerializationSchema.class);

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            String element,
            KafkaSinkContext context,
            Long timestamp) {

        try {
            // 1. 解析 JSON
            JSONObject json = JSON.parseObject(element);

            // 2. 获取目标 Topic
            String targetTopic = json.getString("sink_topic");

            if (targetTopic == null || targetTopic.isEmpty()) {
                logger.error("sink_topic 为空: {}", element);
                return null;
            }

            // 3. 创建 ProducerRecord
            return new ProducerRecord<>(
                targetTopic,                    // Topic
                null,                           // Key
                element.getBytes()              // Value
            );

        } catch (Exception e) {
            logger.error("序列化失败: {}", element, e);
            return null;
        }
    }
}
