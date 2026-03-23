package com.music.bean;

public class AiOverviewStats {
    private String windowStart;
    private String windowEnd;
    private Long requestCount;
    private Long successCount;
    private Long avgLatencyMs;
    private Long likeCount;
    private Long dislikeCount;
    private Long followupCount;
    private Long clickCount;

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

    public Long getRequestCount() {
        return requestCount;
    }

    public void setRequestCount(Long requestCount) {
        this.requestCount = requestCount;
    }

    public Long getSuccessCount() {
        return successCount;
    }

    public void setSuccessCount(Long successCount) {
        this.successCount = successCount;
    }

    public Long getAvgLatencyMs() {
        return avgLatencyMs;
    }

    public void setAvgLatencyMs(Long avgLatencyMs) {
        this.avgLatencyMs = avgLatencyMs;
    }

    public Long getLikeCount() {
        return likeCount;
    }

    public void setLikeCount(Long likeCount) {
        this.likeCount = likeCount;
    }

    public Long getDislikeCount() {
        return dislikeCount;
    }

    public void setDislikeCount(Long dislikeCount) {
        this.dislikeCount = dislikeCount;
    }

    public Long getFollowupCount() {
        return followupCount;
    }

    public void setFollowupCount(Long followupCount) {
        this.followupCount = followupCount;
    }

    public Long getClickCount() {
        return clickCount;
    }

    public void setClickCount(Long clickCount) {
        this.clickCount = clickCount;
    }
}
