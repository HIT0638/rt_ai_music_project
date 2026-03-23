package com.music.bean;

import com.alibaba.fastjson.JSON;

import java.util.Collections;
import java.util.List;

public class AiResponseWideRecord {
    private String eventId;
    private String eventType;
    private String eventTime;
    private String requestId;
    private String sessionId;
    private Long userId;
    private String intentType;
    private String intentGroup;
    private String answerText;
    private Boolean success;
    private String responseTime;
    private Long latencyMs;
    private String modelName;
    private String promptTemplateId;
    private String responseSource;
    private String resultSongIds;
    private String resultSingerNames;
    private Integer resultSongCount;
    private Integer resultSingerCount;
    private Boolean hasSongResult;
    private Boolean hasSingerResult;
    private String firstSongId;
    private String firstSingerName;

    public static AiResponseWideRecord createWideRecord(AiResponseRecord responseRecord) {
        AiResponseWideRecord wideRecord = new AiResponseWideRecord();
        List<String> songIds = parseStringList(responseRecord.getResultSongIds());
        List<String> singerNames = parseStringList(responseRecord.getResultSingerNames());

        wideRecord.setEventId(responseRecord.getEventId());
        wideRecord.setEventType(responseRecord.getEventType());
        wideRecord.setEventTime(responseRecord.getEventTime());
        wideRecord.setRequestId(responseRecord.getRequestId());
        wideRecord.setSessionId(responseRecord.getSessionId());
        wideRecord.setUserId(responseRecord.getUserId());
        wideRecord.setIntentType(responseRecord.getIntentType());
        wideRecord.setIntentGroup(responseRecord.getIntentGroup());
        wideRecord.setAnswerText(responseRecord.getAnswerText());
        wideRecord.setSuccess(responseRecord.getSuccess());
        wideRecord.setResponseTime(responseRecord.getResponseTime());
        wideRecord.setLatencyMs(responseRecord.getLatencyMs());
        wideRecord.setModelName(responseRecord.getModelName());
        wideRecord.setPromptTemplateId(responseRecord.getPromptTemplateId());
        wideRecord.setResponseSource(responseRecord.getResponseSource());
        wideRecord.setResultSongIds(responseRecord.getResultSongIds());
        wideRecord.setResultSingerNames(responseRecord.getResultSingerNames());
        wideRecord.setResultSongCount(songIds.size());
        wideRecord.setResultSingerCount(singerNames.size());
        wideRecord.setHasSongResult(!songIds.isEmpty());
        wideRecord.setHasSingerResult(!singerNames.isEmpty());
        wideRecord.setFirstSongId(songIds.isEmpty() ? null : songIds.get(0));
        wideRecord.setFirstSingerName(singerNames.isEmpty() ? null : singerNames.get(0));

        return wideRecord;
    }

    private static List<String> parseStringList(String value) {
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<String> list = JSON.parseArray(value, String.class);
        return list == null ? Collections.emptyList() : list;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }

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

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
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

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public String getResponseTime() {
        return responseTime;
    }

    public void setResponseTime(String responseTime) {
        this.responseTime = responseTime;
    }

    public Long getLatencyMs() {
        return latencyMs;
    }

    public void setLatencyMs(Long latencyMs) {
        this.latencyMs = latencyMs;
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public String getPromptTemplateId() {
        return promptTemplateId;
    }

    public void setPromptTemplateId(String promptTemplateId) {
        this.promptTemplateId = promptTemplateId;
    }

    public String getResponseSource() {
        return responseSource;
    }

    public void setResponseSource(String responseSource) {
        this.responseSource = responseSource;
    }

    public String getResultSongIds() {
        return resultSongIds;
    }

    public void setResultSongIds(String resultSongIds) {
        this.resultSongIds = resultSongIds;
    }

    public String getResultSingerNames() {
        return resultSingerNames;
    }

    public void setResultSingerNames(String resultSingerNames) {
        this.resultSingerNames = resultSingerNames;
    }

    public Integer getResultSongCount() {
        return resultSongCount;
    }

    public void setResultSongCount(Integer resultSongCount) {
        this.resultSongCount = resultSongCount;
    }

    public Integer getResultSingerCount() {
        return resultSingerCount;
    }

    public void setResultSingerCount(Integer resultSingerCount) {
        this.resultSingerCount = resultSingerCount;
    }

    public Boolean getHasSongResult() {
        return hasSongResult;
    }

    public void setHasSongResult(Boolean hasSongResult) {
        this.hasSongResult = hasSongResult;
    }

    public Boolean getHasSingerResult() {
        return hasSingerResult;
    }

    public void setHasSingerResult(Boolean hasSingerResult) {
        this.hasSingerResult = hasSingerResult;
    }

    public String getFirstSongId() {
        return firstSongId;
    }

    public void setFirstSongId(String firstSongId) {
        this.firstSongId = firstSongId;
    }

    public String getFirstSingerName() {
        return firstSingerName;
    }

    public void setFirstSingerName(String firstSingerName) {
        this.firstSingerName = firstSingerName;
    }
}
