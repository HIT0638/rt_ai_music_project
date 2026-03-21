package com.music.bean;

/**
 * ODS层原始日志(一级解析)
 */
public class UserLogRaw {
    private String timestamp;           // 秒级时间戳
    private String mid;                 // 机器ID
    private String event_type;          // 事件类型
    private String event_content;       // 事件内容(JSON字符串,待二次解析)
    private String ui_version;          // UI版本
    private String machine_version;     // 机器版本

    // 临时字段，用于存储解析后的EventContent对象
    private EventContent eventContentObject;

    public UserLogRaw() {}

    public UserLogRaw(String timestamp, String mid, String event_type, String event_content, String ui_version, String machine_version, EventContent eventContentObject) {
        this.timestamp = timestamp;
        this.mid = mid;
        this.event_type = event_type;
        this.event_content = event_content;
        this.ui_version = ui_version;
        this.machine_version = machine_version;
        this.eventContentObject = eventContentObject;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getMid() {
        return mid;
    }

    public void setMid(String mid) {
        this.mid = mid;
    }

    public String getEvent_type() {
        return event_type;
    }

    public void setEvent_type(String event_type) {
        this.event_type = event_type;
    }

    public String getEvent_content() {
        return event_content;
    }

    public void setEvent_content(String event_content) {
        this.event_content = event_content;
    }

    public String getUi_version() {
        return ui_version;
    }

    public void setUi_version(String ui_version) {
        this.ui_version = ui_version;
    }

    public String getMachine_version() {
        return machine_version;
    }

    public void setMachine_version(String machine_version) {
        this.machine_version = machine_version;
    }

    public EventContent getEventContentObject() {
        return eventContentObject;
    }

    public void setEventContentObject(EventContent eventContentObject) {
        this.eventContentObject = eventContentObject;
    }
}
