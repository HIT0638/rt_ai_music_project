package com.music.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.music.bean.MachineConsumeDetailRecord;
import com.music.bean.MachineConsumeWideRecord;
import com.music.util.ConfigUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class DwmMachineConsumeWideApp {

    private static final String PHOENIX_URL = "jdbc:phoenix:localhost:2181";
    private static final String MACHINE_LOCAL_LOOKUP_SQL =
        "SELECT MID, PROVINCE, CITY, ADDRESS FROM DIM_MACHINE_LOCAL_INFO WHERE MID = ?";
    private static final String MACHINE_ADMIN_LOOKUP_SQL =
        "SELECT MACHINE_NUM, MACHINE_NAME, PACKAGE_NAME, INV_RATE, AGE_RATE, COM_RATE, PAR_RATE "
            + "FROM DIM_MACHINE_ADMIN_MAP WHERE MACHINE_NUM = ?";

    public static void main(String[] args) throws Exception {
        ConfigUtil.init(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableClosureCleaner();
        env.setParallelism(ConfigUtil.getInt("flink.parallelism", 2));

        String kafkaServers = ConfigUtil.get("kafka.bootstrap.servers");
        String dwdTopicName = ConfigUtil.get(
            "kafka.dwd.machine.consume.detail",
            "dwd_machine_consume_detail_rt");
        String dwmTopicName = ConfigUtil.get(
            "kafka.dwm.machine.consume.wide",
            "dwm_machine_consume_wide_rt");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaServers)
            .setTopics(dwdTopicName)
            .setGroupId("dwm-machine-consume-wide-group")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<MachineConsumeDetailRecord> detailStream = env
            .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .name("DWD Machine Consume Detail Source")
            .map(DwmMachineConsumeWideApp::parseDetailRecord)
            .name("Parse Machine Consume Detail")
            .filter(DwmMachineConsumeWideApp::isValidDetailRecord)
            .name("Filter Valid Machine Consume Detail");

        DataStream<MachineConsumeWideRecord> wideStream = detailStream
            .map(new MachineConsumeDimEnrichmentFunction())
            .name("Machine Consume Dim Enrichment");

        wideStream
            .map(JSON::toJSONString)
            .sinkTo(createKafkaSink(kafkaServers, dwmTopicName))
            .name("DWM Machine Consume Wide Sink");

        env.execute("DWM Machine Consume Wide App");
    }

    private static MachineConsumeDetailRecord parseDetailRecord(String value) {
        JSONObject object = JSON.parseObject(value);

        MachineConsumeDetailRecord record = new MachineConsumeDetailRecord();
        record.setId(object.getLong("id"));
        record.setMid(object.getLong("mid"));
        record.setUid(object.getLong("uid"));
        record.setAmount(object.getLong("amount"));
        record.setPkgId(object.getInteger("pkg_id"));
        record.setPkgName(object.getString("pkg_name"));
        record.setActivityId(object.getInteger("activity_id"));
        record.setActivityName(object.getString("activity_name"));
        record.setActionTime(object.getString("action_time"));
        record.setBillDate(object.getString("bill_date"));
        record.setType(object.getString("type"));
        record.setTs(object.getLong("ts"));
        return record;
    }

    private static boolean isValidDetailRecord(MachineConsumeDetailRecord value) {
        return value != null
            && value.getMid() != null
            && value.getAmount() != null
            && value.getActionTime() != null
            && "insert".equals(value.getType());
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

    private static class MachineConsumeDimEnrichmentFunction
        extends RichMapFunction<MachineConsumeDetailRecord, MachineConsumeWideRecord> {

        private transient Connection conn;
        private transient PreparedStatement machineLocalPst;
        private transient PreparedStatement machineAdminPst;

        @Override
        public void open(Configuration parameters) throws Exception {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(PHOENIX_URL);
            machineLocalPst = conn.prepareStatement(MACHINE_LOCAL_LOOKUP_SQL);
            machineAdminPst = conn.prepareStatement(MACHINE_ADMIN_LOOKUP_SQL);
        }

        @Override
        public MachineConsumeWideRecord map(MachineConsumeDetailRecord value) throws Exception {
            MachineConsumeWideRecord record = createBaseWideRecord(value);

            Long mid = value.getMid();
            // 当前 Phoenix DIM 表是动态建表，主键和字段统一按 VARCHAR 落表，
            // 所以这里查询条件也要按字符串绑定。
            machineLocalPst.setString(1, String.valueOf(mid));
            try (ResultSet localResults = machineLocalPst.executeQuery()) {
                if (localResults.next()) {
                    record.setProvince(localResults.getString("PROVINCE"));
                    record.setCity(localResults.getString("CITY"));
                }
            }

            // TODO 1: 如果后面第一版营收口径只做到地区营收，这里补 province/city 就够了
            // TODO 2: 如果要引入分成或机器管理信息，再查询 DIM_MACHINE_ADMIN_MAP
            // TODO 3: 当前 Phoenix 实际验证字段里没有 MAP_CLASS / SCENE_ADDRESS，不要按旧文档字段直接写
            // TODO 3: 如果你想把 actionTime 提前转成事件时间字段，也可以在这里新增 actionTimestampMillis

            return record;
        }

        private MachineConsumeWideRecord createBaseWideRecord(MachineConsumeDetailRecord value) {
            MachineConsumeWideRecord record = new MachineConsumeWideRecord();
            record.setId(value.getId());
            record.setMid(value.getMid());
            record.setUid(value.getUid());
            record.setAmount(value.getAmount());
            record.setPkgId(value.getPkgId());
            record.setPkgName(value.getPkgName());
            record.setActivityId(value.getActivityId());
            record.setActivityName(value.getActivityName());
            record.setActionTime(value.getActionTime());
            record.setBillDate(value.getBillDate());
            return record;
        }

        @Override
        public void close() throws Exception {
            if (machineLocalPst != null) {
                machineLocalPst.close();
            }
            if (machineAdminPst != null) {
                machineAdminPst.close();
            }
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }
}
