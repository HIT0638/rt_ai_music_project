package com.music.bean;

import java.io.Serializable;

public class SongHotStats implements Serializable {
    private static final long serialVersionUID = 1L;

    private String songId;
    private String songName;
    private Long playCount;
    private String windowStart;
    private String windowEnd;

    public SongHotStats() {
    }

    public SongHotStats(String songId, String songName, Long playCount, String windowStart, String windowEnd) {
        this.songId = songId;
        this.songName = songName;
        this.playCount = playCount;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
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

    public Long getPlayCount() {
        return playCount;
    }

    public void setPlayCount(Long playCount) {
        this.playCount = playCount;
    }

    public String getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(String windowStart) {
        this.windowStart = windowStart;
    }

    public String getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(String windowEnd) {
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "SongHotStats{" +
                "songId='" + songId + '\'' +
                ", songName='" + songName + '\'' +
                ", playCount=" + playCount +
                ", windowStart='" + windowStart + '\'' +
                ", windowEnd='" + windowEnd + '\'' +
                '}';
    }
}
