package com.music.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.music.bean.UserLogClean;
import com.music.bean.UserLoginLocationInfo;
import com.music.function.SplitProcessFunction;
import com.music.util.ConfigUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

/**
 * DWD层: ODS -> DWD 数据清洗与两层分流
 *
 * 功能:
 * 1. 从Kafka读取ODS层数据(Base64编码的JSON数组)
 * 2. Base64解码 + flatMap展开
 * 3. 解析JSON + 过滤脏数据
 * 4. 两层分流(在字段转换之前!):
 *    - 第一层: 按event_type分流
 *    - 第二层: 按optrate_type分流 (play/pause/skip)
 * 5. 各自分支进行字段转换
 * 6. 分别写入不同的Kafka Topic
 */
public class DwdUserLogApp {
    public static void main(String[] args) throws Exception {
        // 1. 初始化配置
        ConfigUtil.init(args);

        // 2. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ConfigUtil.getInt("flink.parallelism", 2));

        // 3. 创建Kafka Source
        String kafkaServers = ConfigUtil.get("kafka.bootstrap.servers");
        String odsTopicName = ConfigUtil.get("kafka.ods.log.userlog");
        String groupId = ConfigUtil.get("kafka.group.id");

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaServers)
            .setTopics(odsTopicName)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        // 4. 数据处理流程
        // Step 1: Base64解码 + flatMap展开
        DataStream<String> jsonStream = env
            .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .flatMap((String base64Json, org.apache.flink.util.Collector<String> out) -> {
                try {
                    // Base64解码
                    byte[] decoded = Base64.getDecoder().decode(base64Json);
                    String json = new String(decoded, StandardCharsets.UTF_8);

                    // JSON数组解析
                    JSONArray jsonArray = JSON.parseArray(json);

                    // 展开数组
                    for (int i = 0; i < jsonArray.size(); i++) {
                        out.collect(jsonArray.getString(i));
                    }
                } catch (Exception e) {
                    System.err.println("解码失败: " + base64Json);
                }
            })
            .returns(Types.STRING)
            .name("Base64 Decode");

        // Step 2: 解析JSON + 过滤脏数据
        DataStream<String> filteredJsonStream = jsonStream
            .filter(jsonStr -> {
                try {
                    JSONObject obj = JSON.parseObject(jsonStr);

                    // 只保留播放操作事件，且字段完整
                    if (isValidSongPlayEvent(obj)){
                        return true;
                    } else if (isValidLoginLocationEvent(obj)) {
                        return true;
                    }
                    return false;
                } catch (Exception e) {
                    return false;
                }
            })
            .name("Filter Dirty Data");

        // Step 3: 两层分流(在字段转换之前，使用原始JSON格式)
        SingleOutputStreamOperator<String> processedStream = filteredJsonStream
            .process(new SplitProcessFunction())
            .name("Split Process");

        // Step 4: 获取侧输出流
        DataStream<String> playStream = processedStream.getSideOutput(SplitProcessFunction.PLAY_TAG);
        DataStream<String> pauseStream = processedStream.getSideOutput(SplitProcessFunction.PAUSE_TAG);
        DataStream<String> skipStream = processedStream.getSideOutput(SplitProcessFunction.SKIP_TAG);
        DataStream<String> loginLocationStream = processedStream.getSideOutput(SplitProcessFunction.LOGIN_LOCATION_TAG);
        DataStream<String> otherStream = processedStream.getSideOutput(SplitProcessFunction.OTHER_TAG);
        // Step 5: 合并 pause/skip/other 到 otherAllStream
        DataStream<String> otherAllStream = pauseStream
            .union(skipStream)
            .union(otherStream);

        // Step 6: 各自分支进行字段转换并写入Kafka
        String playTopicName = ConfigUtil.get("kafka.dwd.user.playsong");
        String loginLocationTopicName = ConfigUtil.get("kafka.dwd.user.login.location");
        String otherTopicName = ConfigUtil.get("kafka.dwd.other.userlog");

        // Play分支: 转换后写入dwd_user_playsong_rt
        playStream
            .map(jsonStr -> convertToClean(jsonStr, "play"))
            .map(userLogClean -> JSON.toJSONString(userLogClean))
            .sinkTo(createKafkaSink(kafkaServers, playTopicName))
            .name("Play Sink");

        // LoginLocation分支：转换后写入dwd_user_login_location
        loginLocationStream
                .map(DwdUserLogApp::convertToUserLoginLocation)
                .map(JSON::toJSONString)
                .sinkTo(createKafkaSink(kafkaServers, loginLocationTopicName))
                .name("Login Location Sink");


        // Other分支: 转换后写入dwd_other_userlog_rt
        otherAllStream
            .map(jsonStr -> {
                // 从JSON中提取action
                JSONObject obj = JSON.parseObject(jsonStr);
                JSONObject eventContent = obj.getJSONObject("event_content");
                Integer oprateType = eventContent.getInteger("optrate_type");
                String action = "other";
                if (oprateType != null) {
                    if (oprateType == 1) action = "pause";
                    else if (oprateType == 2) action = "skip";
                }
                return convertToClean(jsonStr, action);
            })
            .map(JSON::toJSONString)
            .sinkTo(createKafkaSink(kafkaServers, otherTopicName))
            .name("Other Sink");

        // 7. 执行任务
        env.execute("DWD User Log App");
    }

    /**
     * 将原始JSON转换为UserLogClean对象
     */
    private static UserLogClean convertToClean(String jsonStr, String action) {
        JSONObject obj = JSON.parseObject(jsonStr);
        JSONObject eventContent = obj.getJSONObject("event_content");

        UserLogClean clean = new UserLogClean();
        clean.setUserId(eventContent.getLong("uid"));
        clean.setSongId(eventContent.getString("songid"));

        // 机器ID优先用mid字段，没有则用event_content.mid
        if (obj.getString("mid") != null) {
            clean.setMachineId(Long.parseLong(obj.getString("mid")));
        } else {
            clean.setMachineId(eventContent.getInteger("mid").longValue());
        }

        clean.setAction(action);

        // 时间戳转换(秒 -> 毫秒)
        long timestamp = Long.parseLong(obj.getString("timestamp")) * 1000;
        clean.setTimestamp(timestamp);

        // 新增date和hour字段
        LocalDateTime dateTime = LocalDateTime.ofInstant(
            Instant.ofEpochMilli(timestamp),
            ZoneId.systemDefault()
        );
        clean.setDate(dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        clean.setHour(dateTime.format(DateTimeFormatter.ofPattern("HH")));

        clean.setIp("");
        clean.setCity("");

        return clean;
    }

    private static UserLoginLocationInfo convertToUserLoginLocation(String jsonStr) {
        JSONObject obj = JSON.parseObject(jsonStr);
        String eventId = obj.getString("event_id");
        Long uid = obj.getLong("uid");
        String eventType = obj.getString("event_type");
        String lng = obj.getString("lng");
        String lat = obj.getString("lat");
        Long loginDt = obj.getLong("login_dt");
        Long mid = obj.getLong("mid");
        // 还有date & hour

        UserLoginLocationInfo userLoginLocationInfo = new UserLoginLocationInfo();
        userLoginLocationInfo.setEventId(eventId);
        userLoginLocationInfo.setEventType(eventType);
        userLoginLocationInfo.setUserId(uid);
        userLoginLocationInfo.setLng(lng);
        userLoginLocationInfo.setLat(lat);
        userLoginLocationInfo.setLoginDt(loginDt);
        userLoginLocationInfo.setMachineId(mid);
        if (loginDt != null) {
            LocalDateTime dateTime = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(loginDt),
                ZoneId.systemDefault()
            );
            userLoginLocationInfo.setDate(dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
            userLoginLocationInfo.setHour(dateTime.format(DateTimeFormatter.ofPattern("HH")));
        }

        return userLoginLocationInfo;
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

    private static boolean isValidSongPlayEvent(JSONObject obj) {
        String eventType = obj.getString("event_type");
        if (!"MINIK_CLIENT_SONG_PLAY_OPERATE_REQ".equals(eventType)) {
            return false;
        }

        JSONObject eventContent = obj.getJSONObject("event_content");
        return eventContent != null
            && eventContent.getString("songid") != null
            && eventContent.getLong("uid") != null;
    }

    private static boolean isValidLoginLocationEvent(JSONObject obj) {
        String eventType = obj.getString("event_type");
        return "USER_LOGIN_LOCATION_LOG".equals(eventType)
            && obj.getString("event_id") != null
            && obj.getLong("uid") != null
            && obj.getString("lng") != null
            && obj.getString("lat") != null
            && obj.getLong("login_dt") != null
            && obj.getString("mid") != null;
    }
}
