package com.music.bean;

import java.io.Serializable;

public class SingerHotStats implements Serializable {
    private static final long serialVersionUID = 1L;

    private String singerInfo;
    private Long playCount;
    private String windowStart;
    private String windowEnd;

    public SingerHotStats() {
    }

    public SingerHotStats(String singerInfo, Long playCount, String windowStart, String windowEnd) {
        this.singerInfo = singerInfo;
        this.playCount = playCount;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    public String getSingerInfo() {
        return singerInfo;
    }

    public void setSingerInfo(String singerInfo) {
        this.singerInfo = singerInfo;
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
        return "SingerHotStats{" +
            "singerInfo='" + singerInfo + '\'' +
            ", playCount=" + playCount +
            ", windowStart='" + windowStart + '\'' +
            ", windowEnd='" + windowEnd + '\'' +
            '}';
    }
}
