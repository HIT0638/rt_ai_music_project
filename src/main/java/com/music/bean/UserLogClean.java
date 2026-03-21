package com.music.bean;

/**
 * 清洗后的用户行为日志(DWD层)
 */
public class UserLogClean {
    private Long userId;
    private String songId;      // 歌曲ID (String类型,如"LX_nk123456")
    private Long machineId;
    private String action;      // 操作类型: play/pause/skip/other
    private Long timestamp;
    private String ip;
    private String city;

    // DWD层新增字段
    private String date;        // 日期(yyyy-MM-dd)
    private String hour;        // 小时(HH)

    public UserLogClean() {}

    public UserLogClean(Long userId, String songId, Long machineId, String action, Long timestamp, String ip, String city, String date, String hour) {
        this.userId = userId;
        this.songId = songId;
        this.machineId = machineId;
        this.action = action;
        this.timestamp = timestamp;
        this.ip = ip;
        this.city = city;
        this.date = date;
        this.hour = hour;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getSongId() {
        return songId;
    }

    public void setSongId(String songId) {
        this.songId = songId;
    }

    public Long getMachineId() {
        return machineId;
    }

    public void setMachineId(Long machineId) {
        this.machineId = machineId;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }
}
