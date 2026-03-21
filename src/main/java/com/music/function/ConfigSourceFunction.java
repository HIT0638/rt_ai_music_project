package com.music.function;

import com.music.bean.TblConfig;
import com.music.util.ConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 从 MySQL 读取 config_info 配置表
 * 一次性读取，不持续运行
 */
public class ConfigSourceFunction extends RichSourceFunction<TblConfig> {

    private static final Logger logger = LoggerFactory.getLogger(ConfigSourceFunction.class);
    private volatile boolean isRunning = true;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        logger.info("ConfigSourceFunction 初始化");
    }

    @Override
    public void run(SourceContext<TblConfig> ctx) throws Exception {
        // 1. 获取 MySQL 连接信息
        String url = ConfigUtil.get("mysql.url");
        String user = ConfigUtil.get("mysql.user");
        String password = ConfigUtil.get("mysql.password");

        // 2. 连接 MySQL
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            logger.info("成功连接到 MySQL");

            // 3. 查询 config_info 表
            String sql = "SELECT tbl_name, tbl_db, tbl_type, pk_col, cols, " +
                        "phoenix_tbl_name, sink_topic FROM config_info";

            try (PreparedStatement pstmt = conn.prepareStatement(sql);
                 ResultSet rs = pstmt.executeQuery()) {

                // 4. 逐行读取并 emit
                int count = 0;
                while (rs.next() && isRunning) {
                    TblConfig config = new TblConfig(
                        rs.getString("tbl_name"),
                        rs.getString("tbl_db"),
                        rs.getString("tbl_type"),
                        rs.getString("pk_col"),
                        rs.getString("cols"),
                        rs.getString("phoenix_tbl_name"),
                        rs.getString("sink_topic")
                    );

                    ctx.collect(config);
                    count++;
                    logger.info("读取配置: {}", config);
                }

                logger.info("共读取 {} 条配置", count);
            }
        } catch (Exception e) {
            logger.error("读取 config_info 失败", e);
            throw e;
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        logger.info("ConfigSourceFunction 取消");
    }
}
