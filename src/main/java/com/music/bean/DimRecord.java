package com.music.bean;

import java.io.Serializable;
import java.util.Map;

/**
 * 维度记录 Bean 类
 * 用于解析 dwd_dim_info_rt 中的数据
 */
public class DimRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private String type;                    // insert/update/delete/bootstrap-insert
    private String ts;                      // 时间戳
    private String pkCol;                   // 主键列名
    private String cols;                    // 需要同步的列（逗号分隔）
    private String phoenixTableName;        // Phoenix 表名
    private String sinkTopic;               // Sink Topic
    private Map<String, Object> data;       // 实际数据

    public DimRecord() {
    }

    public DimRecord(String type, String ts, String pkCol, String cols,
                     String phoenixTableName, String sinkTopic, Map<String, Object> data) {
        this.type = type;
        this.ts = ts;
        this.pkCol = pkCol;
        this.cols = cols;
        this.phoenixTableName = phoenixTableName;
        this.sinkTopic = sinkTopic;
        this.data = data;
    }

    // Getters and Setters
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }

    public String getPkCol() {
        return pkCol;
    }

    public void setPkCol(String pkCol) {
        this.pkCol = pkCol;
    }

    public String getCols() {
        return cols;
    }

    public void setCols(String cols) {
        this.cols = cols;
    }

    public String getPhoenixTableName() {
        return phoenixTableName;
    }

    public void setPhoenixTableName(String phoenixTableName) {
        this.phoenixTableName = phoenixTableName;
    }

    public String getSinkTopic() {
        return sinkTopic;
    }

    public void setSinkTopic(String sinkTopic) {
        this.sinkTopic = sinkTopic;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "DimRecord{" +
                "type='" + type + '\'' +
                ", ts='" + ts + '\'' +
                ", pkCol='" + pkCol + '\'' +
                ", cols='" + cols + '\'' +
                ", phoenixTableName='" + phoenixTableName + '\'' +
                ", sinkTopic='" + sinkTopic + '\'' +
                ", data=" + data +
                '}';
    }
}
