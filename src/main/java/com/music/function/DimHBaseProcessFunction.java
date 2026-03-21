package com.music.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.music.util.ETLUtil;
import com.music.util.RedisUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 维度数据 HBase 处理函数
 * 功能：
 * 1. 动态创建 Phoenix 表
 * 2. 对数据进行 ETL 清洗
 * 3. Upsert/Delete 数据到 HBase
 */
public class DimHBaseProcessFunction extends KeyedProcessFunction<String, String, Void> {

    private static final Logger logger = LoggerFactory.getLogger(DimHBaseProcessFunction.class);

    // 状态：记录已创建的表
    private transient ValueState<String> phxTableState;

    // Phoenix 连接
    private transient Connection phoenixConn;
    private transient JedisPool jedisPool;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化状态
        ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>(
            "phxTableState",
            String.class
        );
        phxTableState = getRuntimeContext().getState(descriptor);

        // 初始化 Phoenix 连接
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            String phoenixUrl = "jdbc:phoenix:localhost:2181";
            phoenixConn = DriverManager.getConnection(phoenixUrl);
            phoenixConn.setAutoCommit(true);
            jedisPool = RedisUtil.createPool();
            logger.info("Phoenix 连接初始化成功");
        } catch (Exception e) {
            logger.error("Phoenix 连接初始化失败", e);
            throw new RuntimeException("Phoenix 连接初始化失败", e);
        }
    }

    @Override
    public void processElement(String value, Context ctx, Collector<Void> out) throws Exception {
        try {
            // 1. 解析数据
            JSONObject nObject = JSON.parseObject(value);
            String type = nObject.getString("type");
            String phoenixTableName = nObject.getString("phoenix_table_name");
            String pkCol = nObject.getString("pk_col");
            String cols = nObject.getString("cols");

            logger.debug("处理数据: type={}, table={}, pkCol={}", type, phoenixTableName, pkCol);

            // 2. 只处理 insert/update/bootstrap-insert，忽略 delete
            if ("insert".equals(type) || "update".equals(type) || "bootstrap-insert".equals(type)) {

                // 3. 检查状态，如果表未创建则创建表
                if (phxTableState.value() == null) {
                    createPhoenixTable(phoenixTableName, pkCol, cols);
                    phxTableState.update(phoenixTableName);
                    logger.info("表 {} 创建成功，状态已更新", phoenixTableName);
                }

                // 4. 获取主键值
                String pkColValue = nObject.getString(pkCol);

                // 5. 对特定表进行 ETL 清洗
                if ("DIM_SONG".equals(phoenixTableName)) {
                    nObject.put("singer_info", ETLUtil.getSingerName(nObject.get("singer_info")));
                    nObject.put("album", ETLUtil.getAlbumName(nObject.get("album")));
                    nObject.put("authorized_company", ETLUtil.getAuthCompanyName(nObject.get("authorized_company")));
                    logger.debug("DIM_SONG 数据清洗完成");
                }

                // 6. Upsert 数据到 Phoenix
                upsertToPhoenix(phoenixTableName, pkCol, cols, nObject);
                logger.debug("数据 upsert 成功: table={}, pkColValue={}", phoenixTableName, pkColValue);

                if ("update".equals(type)) {
                    invalidateSongCacheIfNeeded(phoenixTableName, pkColValue);
                }

            } else if ("delete".equals(type)) {
                // 处理 delete 操作
                String pkColValue = nObject.getString(pkCol);
                deleteFromPhoenix(phoenixTableName, pkCol, pkColValue);
                logger.debug("数据删除成功: table={}, pkColValue={}", phoenixTableName, pkColValue);
                invalidateSongCacheIfNeeded(phoenixTableName, pkColValue);
            }

        } catch (Exception e) {
            logger.error("处理数据失败: {}", value, e);
            throw e;
        }
    }

    /**
     * 创建 Phoenix 表
     */
    private void createPhoenixTable(String phoenixTableName, String pkCol, String cols) throws Exception {
        // 拼接建表 SQL
        StringBuilder createSQL = new StringBuilder(
            String.format("CREATE TABLE IF NOT EXISTS %s (%s VARCHAR PRIMARY KEY", phoenixTableName, pkCol)
        );

        // 拼接列定义
        for (String col : cols.split(",")) {
            createSQL.append(", ").append(col.trim()).append(" VARCHAR");
        }
        createSQL.append(")");

        logger.info("执行建表 SQL: {}", createSQL.toString());

        try {
            PreparedStatement pst = phoenixConn.prepareStatement(createSQL.toString());
            pst.execute();
            pst.close();
            logger.info("表 {} 创建成功", phoenixTableName);
        } catch (Exception e) {
            logger.error("创建表 {} 失败", phoenixTableName, e);
            throw e;
        }
    }

    /**
     * Upsert 数据到 Phoenix
     */
    private void upsertToPhoenix(String phoenixTableName, String pkCol, String cols, JSONObject data) throws Exception {
        // 获取主键值
        String pkColValue = data.getString(pkCol);

        // 拼接 upsert SQL
        StringBuilder upsertSQL = new StringBuilder(
            String.format("UPSERT INTO %s VALUES ('%s'", phoenixTableName, pkColValue)
        );

        // 拼接列值
        for (String col : cols.split(",")) {
            String colValue = data.getString(col.trim());
            // 处理 null 值和特殊字符
            if (colValue == null) {
                colValue = "";
            }
            // 转义单引号
            colValue = colValue.replace("'", "''");
            upsertSQL.append(", '").append(colValue).append("'");
        }
        upsertSQL.append(")");

        logger.debug("执行 upsert SQL: {}", upsertSQL.toString());

        try {
            PreparedStatement pst = phoenixConn.prepareStatement(upsertSQL.toString());
            pst.execute();
            pst.close();
        } catch (Exception e) {
            logger.error("Upsert 数据失败: {}", upsertSQL.toString(), e);
            throw e;
        }
    }

    /**
     * 删除数据
     */
    private void deleteFromPhoenix(String phoenixTableName, String pkCol, String pkColValue) throws Exception {
        String deleteSQL = String.format(
            "DELETE FROM %s WHERE %s = '%s'",
            phoenixTableName, pkCol, pkColValue
        );

        logger.debug("执行删除 SQL: {}", deleteSQL);

        try {
            PreparedStatement pst = phoenixConn.prepareStatement(deleteSQL);
            pst.execute();
            pst.close();
        } catch (Exception e) {
            logger.error("删除数据失败: {}", deleteSQL, e);
            throw e;
        }
    }

    private void invalidateSongCacheIfNeeded(String phoenixTableName, String pkColValue) {
        if (!"DIM_SONG".equals(phoenixTableName) || pkColValue == null || pkColValue.isEmpty()) {
            return;
        }

        String cacheKey = RedisUtil.buildSongDimKey(pkColValue);
        RedisUtil.delete(jedisPool, cacheKey);
        logger.info("删除歌曲维度缓存成功, key={}", cacheKey);
    }

    @Override
    public void close() throws Exception {
        if (phoenixConn != null && !phoenixConn.isClosed()) {
            phoenixConn.close();
            logger.info("Phoenix 连接已关闭");
        }
        if (jedisPool != null) {
            jedisPool.close();
        }
    }
}
