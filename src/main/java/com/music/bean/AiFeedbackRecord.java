package com.music.bean;

import com.alibaba.fastjson.JSONObject;

public class AiFeedbackRecord {
    private String eventId;
    private String eventType;
    private String eventTime;
    private String requestId;
    private String sessionId;
    private Long userId;
    private String feedbackType;
    private String feedbackTime;

    public static AiFeedbackRecord createAiFeedbackRecord(String value) {
        AiFeedbackRecord aiFeedbackRecord = new AiFeedbackRecord();
        JSONObject jsonObject = JSONObject.parseObject(value);
        JSONObject payload = jsonObject.getJSONObject("payload");
        aiFeedbackRecord.setEventId(jsonObject.getString("eventId"));
        aiFeedbackRecord.setEventType(jsonObject.getString("eventType"));
        aiFeedbackRecord.setEventTime(jsonObject.getString("eventTime"));
        aiFeedbackRecord.setRequestId(jsonObject.getString("requestId"));
        aiFeedbackRecord.setSessionId(jsonObject.getString("sessionId"));
        aiFeedbackRecord.setUserId(jsonObject.getLong("userId"));
        aiFeedbackRecord.setFeedbackType(payload.getString("feedbackType"));
        aiFeedbackRecord.setFeedbackTime(payload.getString("feedbackTime"));
        return aiFeedbackRecord;
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

    public String getFeedbackType() {
        return feedbackType;
    }

    public void setFeedbackType(String feedbackType) {
        this.feedbackType = feedbackType;
    }

    public String getFeedbackTime() {
        return feedbackTime;
    }

    public void setFeedbackTime(String feedbackTime) {
        this.feedbackTime = feedbackTime;
    }
}
