package com.music.util;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigUtil {
    private static Properties props = new Properties();
    private static ParameterTool params;

    static {
        try (InputStream in = ConfigUtil.class
                .getClassLoader().getResourceAsStream("config.properties")) {
            if (in == null) {
                throw new RuntimeException("找不到config.properties");
            }
            props.load(in);
        } catch (IOException e) {
            throw new RuntimeException("加载配置文件失败");
        }
    }

    // 初始化命令行参数
    public static void init(String[] args) {
        params = ParameterTool.fromArgs(args);
    }

    // 获取配置（命令行优先）
    public static String get(String key) {
        if (params != null && params.has(key)) {
            return params.get(key);
        }
        return props.getProperty(key);
    }

    // 获取配置（带默认值）
    public static String get(String key, String defaultValue) {
        String value = get(key);
        return value != null ? value : defaultValue;
    }

    // 获取整数配置
    public static int getInt(String key, int defaultValue) {
        String value = get(key);
        return value != null ? Integer.parseInt(value) : defaultValue;
    }

    // 获取长整数配置
    public static long getLong(String key, long defaultValue) {
        String value = get(key);
        return value != null ? Long.parseLong(value) : defaultValue;
    }
}
