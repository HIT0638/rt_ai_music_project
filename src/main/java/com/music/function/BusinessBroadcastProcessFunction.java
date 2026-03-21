package com.music.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.music.bean.TblConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 广播处理函数
 * 处理主流（业务数据）和广播流（配置数据）
 */
public class BusinessBroadcastProcessFunction
    extends BroadcastProcessFunction<String, TblConfig, String> {

    private static final Logger logger = LoggerFactory.getLogger(BusinessBroadcastProcessFunction.class);
    private final MapStateDescriptor<String, TblConfig> configStateDescriptor;

    public BusinessBroadcastProcessFunction(MapStateDescriptor<String, TblConfig> descriptor) {
        this.configStateDescriptor = descriptor;
    }

    /**
     * 处理主流数据（业务数据）
     */
    @Override
    public void processElement(
            String value,
            ReadOnlyContext ctx,
            Collector<String> out) throws Exception {

        // 1. 解析 Maxwell 格式
        JSONObject maxwellMsg = JSON.parseObject(value);
        String database = maxwellMsg.getString("database");
        String table = maxwellMsg.getString("table");
        String ts = maxwellMsg.getString("ts");
        String type = maxwellMsg.getString("type");
        JSONObject data = maxwellMsg.getJSONObject("data");

        // 如果 data 为 null（DDL 操作），忽略
        if (data == null) {
            logger.debug("忽略 DDL 操作: table={}", table);
            return;
        }

        // 2. 构建配置 Key（database_table）
        String configKey = database + "_" + table;

        // 3. 从广播状态读取配置
        ReadOnlyBroadcastState<String, TblConfig> broadcastState =
            ctx.getBroadcastState(configStateDescriptor);

        if (broadcastState.contains(configKey)) {
            // 4. 获取配置
            TblConfig config = broadcastState.get(configKey);

            // 5. 添加元数据字段
            data.put("ts", ts);
            data.put("pk_col", config.getPkCol());
            data.put("cols", config.getCols());
            data.put("phoenix_table_name", config.getPhoenixTblName());
            data.put("sink_topic", config.getSinkTopic());
            data.put("type", type);

            // 6. 输出
            out.collect(data.toJSONString());

            logger.debug("处理数据: table={}, sink_topic={}", table, config.getSinkTopic());
        } else {
            // 配置中没有这个表，忽略
            logger.warn("未找到配置: {}", configKey);
        }
    }

    /**
     * 处理广播流数据（配置数据）
     */
    @Override
    public void processBroadcastElement(
            TblConfig config,
            Context ctx,
            Collector<String> out) throws Exception {

        // 1. 构建配置 Key（database_table）
        String configKey = config.getTblDb() + "_" + config.getTblName();

        // 2. 更新广播状态
        BroadcastState<String, TblConfig> broadcastState =
            ctx.getBroadcastState(configStateDescriptor);
        broadcastState.put(configKey, config);

        logger.info("更新广播状态: key={}, config={}", configKey, config);
    }
}
