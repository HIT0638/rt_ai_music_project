package com.music.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ETL 数据清洗工具类
 * 用于对维度数据进行清洗和转换
 */
public class ETLUtil {
    private static final Logger logger = LoggerFactory.getLogger(ETLUtil.class);

    /**
     * 清洗歌手信息
     * 从 JSON 字符串中提取歌手名称
     *
     * @param singerInfo 原始歌手信息（JSON 格式或字符串）
     * @return 清洗后的歌手名称
     */
    public static String getSingerName(Object singerInfo) {
        if (singerInfo == null) {
            return "无信息";
        }

        String info = singerInfo.toString().trim();

        // 如果是 JSON 格式，尝试提取 name 字段
        if (info.startsWith("[") && info.contains("name")) {
            try {
                // 简单的正则提取，实际可用 JSON 解析
                int nameStart = info.indexOf("\"name\"");
                if (nameStart != -1) {
                    int valueStart = info.indexOf(":", nameStart) + 1;
                    int valueEnd = info.indexOf("\"", valueStart + 1);
                    if (valueEnd > valueStart) {
                        return info.substring(valueStart + 1, valueEnd);
                    }
                }
            } catch (Exception e) {
                logger.warn("解析歌手信息失败: {}", info, e);
            }
        }

        // 如果是普通字符串，直接返回
        return info.isEmpty() ? "无信息" : info;
    }

    /**
     * 清洗专辑信息
     * 从专辑 ID 或名称中提取有效信息
     *
     * @param albumInfo 原始专辑信息
     * @return 清洗后的专辑信息
     */
    public static String getAlbumName(Object albumInfo) {
        if (albumInfo == null) {
            return "无信息";
        }

        String info = albumInfo.toString().trim();
        return info.isEmpty() ? "无信息" : info;
    }

    /**
     * 清洗发行公司信息
     * 标准化公司名称
     *
     * @param companyInfo 原始公司信息
     * @return 清洗后的公司名称
     */
    public static String getAuthCompanyName(Object companyInfo) {
        if (companyInfo == null) {
            return "无信息";
        }

        String info = companyInfo.toString().trim();
        return info.isEmpty() ? "无信息" : info;
    }

    /**
     * 通用的数据清洗方法
     * 处理 null 值和空字符串
     *
     * @param value 原始值
     * @return 清洗后的值
     */
    public static String cleanValue(Object value) {
        if (value == null) {
            return "无信息";
        }

        String str = value.toString().trim();
        return str.isEmpty() ? "无信息" : str;
    }
}
