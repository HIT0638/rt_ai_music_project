package com.music.bean;

/**
 * 用户行为日志实体类(ODS层原始数据)
 */
public class UserLog {
    private Long userId;        // 用户ID
    private Long songId;        // 歌曲ID
    private Long machineId;     // 机器ID
    private String action;      // 行为类型(play/download/collect)
    private Long timestamp;     // 时间戳
    private String ip;          // IP地址
    private String city;        // 城市

    public UserLog() {}

    public UserLog(Long userId, Long songId, Long machineId, String action, Long timestamp, String ip, String city) {
        this.userId = userId;
        this.songId = songId;
        this.machineId = machineId;
        this.action = action;
        this.timestamp = timestamp;
        this.ip = ip;
        this.city = city;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getSongId() {
        return songId;
    }

    public void setSongId(Long songId) {
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
}
