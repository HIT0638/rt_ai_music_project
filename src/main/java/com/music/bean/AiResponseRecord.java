package com.music.bean;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class AiResponseRecord {
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
    private String resultSongIds;
    private String resultSingerNames;
    private String responseSource;

    public static AiResponseRecord createAiResponseRecord(String value) {
        JSONObject jsonObject = JSON.parseObject(value);
        JSONObject payload = jsonObject.getJSONObject("payload");
        AiResponseRecord aiResponseRecord = new AiResponseRecord();
        aiResponseRecord.setEventId(jsonObject.getString("eventId"));
        aiResponseRecord.setEventType(jsonObject.getString("eventType"));
        aiResponseRecord.setEventTime(jsonObject.getString("eventTime"));
        aiResponseRecord.setRequestId(jsonObject.getString("requestId"));
        aiResponseRecord.setSessionId(jsonObject.getString("sessionId"));
        aiResponseRecord.setUserId(jsonObject.getLong("userId"));
        aiResponseRecord.setIntentType(payload.getString("intentType"));
        aiResponseRecord.setIntentGroup(payload.getString("intentGroup"));
        aiResponseRecord.setAnswerText(payload.getString("answerText"));
        aiResponseRecord.setSuccess(payload.getBoolean("success"));
        aiResponseRecord.setResponseTime(payload.getString("responseTime"));
        aiResponseRecord.setLatencyMs(payload.getLong("latencyMs"));
        aiResponseRecord.setModelName(payload.getString("modelName"));
        aiResponseRecord.setPromptTemplateId(payload.getString("promptTemplateId"));
        aiResponseRecord.setResultSongIds(toJsonString(payload.get("resultSongIds")));
        aiResponseRecord.setResultSingerNames(toJsonString(payload.get("resultSingerNames")));
        aiResponseRecord.setResponseSource(payload.getString("responseSource"));
        return aiResponseRecord;
    }

    private static String toJsonString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return (String) value;
        }
        return JSON.toJSONString(value);
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

    public String getResponseSource() {
        return responseSource;
    }

    public void setResponseSource(String responseSource) {
        this.responseSource = responseSource;
    }
}
