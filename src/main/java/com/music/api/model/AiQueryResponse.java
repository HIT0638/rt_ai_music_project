package com.music.api.model;

import java.util.List;

public class AiQueryResponse {
    private String requestId;
    private String sessionId;
    private String intentType;
    private String intentGroup;
    private String answerText;
    private boolean success;
    private Long latencyMs;
    private String responseSource;
    private List<String> resultSongIds;
    private List<String> resultSingerNames;

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getIntentType() {
        return intentType;
    }

    public void setIntentType(String intentType) {
        this.intentType = intentType;
    }

    public String getIntentGroup() {
        return intentGroup;
    }

    public void setIntentGroup(String intentGroup) {
        this.intentGroup = intentGroup;
    }

    public String getAnswerText() {
        return answerText;
    }

    public void setAnswerText(String answerText) {
        this.answerText = answerText;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public Long getLatencyMs() {
        return latencyMs;
    }

    public void setLatencyMs(Long latencyMs) {
        this.latencyMs = latencyMs;
    }

    public String getResponseSource() {
        return responseSource;
    }

    public void setResponseSource(String responseSource) {
        this.responseSource = responseSource;
    }

    public List<String> getResultSongIds() {
        return resultSongIds;
    }

    public void setResultSongIds(List<String> resultSongIds) {
        this.resultSongIds = resultSongIds;
    }

    public List<String> getResultSingerNames() {
        return resultSingerNames;
    }

    public void setResultSingerNames(List<String> resultSingerNames) {
        this.resultSingerNames = resultSingerNames;
    }
}
