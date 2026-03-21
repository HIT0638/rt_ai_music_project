package com.music.bean;

/**
 * 事件内容(嵌套对象)
 */
public class EventContent {
    private String songid;              // 歌曲ID
    private Integer mid;                // 机器ID
    private Integer optrate_type;       // 操作类型(0=play,1=pause,2=skip)
    private Long uid;                   // 用户ID
    private Integer consume_type;       // 消费类型
    private Integer play_time;          // 播放时长
    private Integer dur_time;           // 持续时长
    private Integer session_id;         // 会话ID
    private String songname;            // 歌曲名称
    private Integer pkg_id;             // 套餐ID
    private String order_id;            // 订单ID

    public EventContent() {}

    public EventContent(String songid, Integer mid, Integer optrate_type, Long uid, Integer consume_type, Integer play_time, Integer dur_time, Integer session_id, String songname, Integer pkg_id, String order_id) {
        this.songid = songid;
        this.mid = mid;
        this.optrate_type = optrate_type;
        this.uid = uid;
        this.consume_type = consume_type;
        this.play_time = play_time;
        this.dur_time = dur_time;
        this.session_id = session_id;
        this.songname = songname;
        this.pkg_id = pkg_id;
        this.order_id = order_id;
    }

    public String getSongid() {
        return songid;
    }

    public void setSongid(String songid) {
        this.songid = songid;
    }

    public Integer getMid() {
        return mid;
    }

    public void setMid(Integer mid) {
        this.mid = mid;
    }

    public Integer getOptrate_type() {
        return optrate_type;
    }

    public void setOptrate_type(Integer optrate_type) {
        this.optrate_type = optrate_type;
    }

    public Long getUid() {
        return uid;
    }

    public void setUid(Long uid) {
        this.uid = uid;
    }

    public Integer getConsume_type() {
        return consume_type;
    }

    public void setConsume_type(Integer consume_type) {
        this.consume_type = consume_type;
    }

    public Integer getPlay_time() {
        return play_time;
    }

    public void setPlay_time(Integer play_time) {
        this.play_time = play_time;
    }

    public Integer getDur_time() {
        return dur_time;
    }

    public void setDur_time(Integer dur_time) {
        this.dur_time = dur_time;
    }

    public Integer getSession_id() {
        return session_id;
    }

    public void setSession_id(Integer session_id) {
        this.session_id = session_id;
    }

    public String getSongname() {
        return songname;
    }

    public void setSongname(String songname) {
        this.songname = songname;
    }

    public Integer getPkg_id() {
        return pkg_id;
    }

    public void setPkg_id(Integer pkg_id) {
        this.pkg_id = pkg_id;
    }

    public String getOrder_id() {
        return order_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }
}
