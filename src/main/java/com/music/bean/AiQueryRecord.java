package com.music.bean;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class AiQueryRecord {
    private String eventId;
    private String eventType;
    private String eventTime;
    private String requestId;
    private String sessionId;
    private Long userId;
    private String queryText;
    private String intentType;
    private String intentGroup;
    private String queryTime;

    public static AiQueryRecord createQueryRecord(String value){
        JSONObject jsonObject = JSON.parseObject(value);
        JSONObject payload = jsonObject.getJSONObject("payload");
        AiQueryRecord record = new AiQueryRecord();
        record.setEventId(jsonObject.getString("eventId"));
        record.setEventType(jsonObject.getString("eventType"));
        record.setEventTime(jsonObject.getString("eventTime"));
        record.setRequestId(jsonObject.getString("requestId"));
        record.setSessionId(jsonObject.getString("sessionId"));
        record.setUserId(jsonObject.getLong("userId"));
        record.setQueryText(payload.getString("queryText"));
        record.setIntentType(payload.getString("intentType"));
        record.setIntentGroup(payload.getString("intentGroup"));
        record.setQueryTime(payload.getString("queryTime"));

        return record;
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

    public String getQueryText() {
        return queryText;
    }

    public void setQueryText(String queryText) {
        this.queryText = queryText;
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

    public String getQueryTime() {
        return queryTime;
    }

    public void setQueryTime(String queryTime) {
        this.queryTime = queryTime;
    }
}
