package com.music.bean;

/**
 * Flume发送到Kafka的数据格式
 */
public class FlumeEvent {
    private String body;  // Base64编码的JSON数据

    public FlumeEvent() {}

    public FlumeEvent(String body) {
        this.body = body;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }
}
