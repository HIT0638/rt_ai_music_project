package com.music.bean;

import java.io.Serializable;

public class SongPlayWideRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private String songId;
    private String songName;
    private String singerInfo;
    private String action;
    private Long timestamp;

    public SongPlayWideRecord() {
    }

    public SongPlayWideRecord(String songId, String songName, String singerInfo, String action, Long timestamp) {
        this.songId = songId;
        this.songName = songName;
        this.singerInfo = singerInfo;
        this.action = action;
        this.timestamp = timestamp;
    }

    public String getSongId() {
        return songId;
    }

    public void setSongId(String songId) {
        this.songId = songId;
    }

    public String getSongName() {
        return songName;
    }

    public void setSongName(String songName) {
        this.songName = songName;
    }

    public String getSingerInfo() {
        return singerInfo;
    }

    public void setSingerInfo(String singerInfo) {
        this.singerInfo = singerInfo;
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
}
