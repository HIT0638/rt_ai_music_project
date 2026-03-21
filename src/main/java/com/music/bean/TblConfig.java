package com.music.bean;

/**
 * config_info 表映射
 * 用于配置业务表的处理规则
 */
public class TblConfig {
    private String tblName;         // 表名
    private String tblDb;           // 数据库名
    private String tblType;         // 表类型（dim/fact）
    private String pkCol;           // 主键列名
    private String cols;            // 需要的列（逗号分隔）
    private String phoenixTblName;  // Phoenix 表名
    private String sinkTopic;       // 输出 Topic

    public TblConfig() {}

    public TblConfig(String tblName, String tblDb, String tblType, String pkCol, String cols, String phoenixTblName, String sinkTopic) {
        this.tblName = tblName;
        this.tblDb = tblDb;
        this.tblType = tblType;
        this.pkCol = pkCol;
        this.cols = cols;
        this.phoenixTblName = phoenixTblName;
        this.sinkTopic = sinkTopic;
    }

    public String getTblName() {
        return tblName;
    }

    public void setTblName(String tblName) {
        this.tblName = tblName;
    }

    public String getTblDb() {
        return tblDb;
    }

    public void setTblDb(String tblDb) {
        this.tblDb = tblDb;
    }

    public String getTblType() {
        return tblType;
    }

    public void setTblType(String tblType) {
        this.tblType = tblType;
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

    public String getPhoenixTblName() {
        return phoenixTblName;
    }

    public void setPhoenixTblName(String phoenixTblName) {
        this.phoenixTblName = phoenixTblName;
    }

    public String getSinkTopic() {
        return sinkTopic;
    }

    public void setSinkTopic(String sinkTopic) {
        this.sinkTopic = sinkTopic;
    }
}
