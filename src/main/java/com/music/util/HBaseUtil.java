package com.music.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * HBase 操作工具类
 * 用于 HBase 表的创建、检查、数据写入等操作
 */
public class HBaseUtil {
    private static final Logger logger = LoggerFactory.getLogger(HBaseUtil.class);

    private static final String DEFAULT_COLUMN_FAMILY = "info";
    private static Configuration config;
    private static Connection connection;

    static {
        try {
            config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", ConfigUtil.get("hbase.zookeeper.quorum", "localhost:2181"));
            config.set("hbase.zookeeper.property.clientPort", ConfigUtil.get("hbase.zookeeper.property.clientPort", "2181"));
            connection = ConnectionFactory.createConnection(config);
            logger.info("HBase 连接初始化成功");
        } catch (IOException e) {
            logger.error("HBase 连接初始化失败", e);
            throw new RuntimeException("HBase 连接初始化失败", e);
        }
    }

    /**
     * 获取 HBase 连接
     *
     * @return Connection 对象
     */
    public static Connection getConnection() {
        return connection;
    }

    /**
     * 检查表是否存在
     *
     * @param tableName 表名
     * @return true 表示存在，false 表示不存在
     */
    public static boolean tableExists(String tableName) {
        try {
            Admin admin = connection.getAdmin();
            boolean exists = admin.tableExists(TableName.valueOf(tableName));
            logger.debug("检查表 {} 是否存在: {}", tableName, exists);
            return exists;
        } catch (IOException e) {
            logger.error("检查表 {} 是否存在失败", tableName, e);
            return false;
        }
    }

    /**
     * 创建表（如果不存在）
     *
     * @param tableName 表名
     * @param columnFamilies 列族名称（可变参数）
     */
    public static void createTableIfNotExists(String tableName, String... columnFamilies) {
        try {
            Admin admin = connection.getAdmin();
            TableName table = TableName.valueOf(tableName);

            if (admin.tableExists(table)) {
                logger.info("表 {} 已存在，跳过创建", tableName);
                return;
            }

            // 如果没有指定列族，使用默认列族
            if (columnFamilies == null || columnFamilies.length == 0) {
                columnFamilies = new String[]{DEFAULT_COLUMN_FAMILY};
            }

            HTableDescriptor tableDescriptor = new HTableDescriptor(table);
            for (String columnFamily : columnFamilies) {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);
                tableDescriptor.addFamily(columnDescriptor);
            }

            admin.createTable(tableDescriptor);
            logger.info("表 {} 创建成功，列族: {}", tableName, String.join(",", columnFamilies));
        } catch (IOException e) {
            logger.error("创建表 {} 失败", tableName, e);
            throw new RuntimeException("创建表 " + tableName + " 失败", e);
        }
    }

    /**
     * Upsert 数据到 HBase
     *
     * @param tableName 表名
     * @param rowKey 行键
     * @param data 数据（Map 格式，key 为列名，value 为列值）
     */
    public static void upsert(String tableName, String rowKey, Map<String, String> data) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));

            for (Map.Entry<String, String> entry : data.entrySet()) {
                String columnName = entry.getKey();
                String columnValue = entry.getValue();
                put.addColumn(
                    Bytes.toBytes(DEFAULT_COLUMN_FAMILY),
                    Bytes.toBytes(columnName),
                    Bytes.toBytes(columnValue)
                );
            }

            table.put(put);
            logger.debug("Upsert 数据到表 {}，rowKey: {}", tableName, rowKey);
        } catch (IOException e) {
            logger.error("Upsert 数据到表 {} 失败，rowKey: {}", tableName, rowKey, e);
            throw new RuntimeException("Upsert 数据失败", e);
        }
    }

    /**
     * 删除数据
     *
     * @param tableName 表名
     * @param rowKey 行键
     */
    public static void delete(String tableName, String rowKey) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            logger.debug("删除数据从表 {}，rowKey: {}", tableName, rowKey);
        } catch (IOException e) {
            logger.error("删除数据从表 {} 失败，rowKey: {}", tableName, rowKey, e);
            throw new RuntimeException("删除数据失败", e);
        }
    }

    /**
     * 关闭连接
     */
    public static void closeConnection() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
                logger.info("HBase 连接已关闭");
            }
        } catch (IOException e) {
            logger.error("关闭 HBase 连接失败", e);
        }
    }
}
