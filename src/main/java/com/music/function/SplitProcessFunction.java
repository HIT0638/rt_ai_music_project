package com.music.function;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * 两层分流处理函数
 *
 * 分流逻辑:
 * 第一层: 按event_type分流
 *   - MINIK_CLIENT_SONG_PLAY_OPERATE_REQ -> 进入第二层分流
 *   - 其他 -> unknownTag
 *
 * 第二层: 按optrate_type分流
 *   - 0 -> play (播放)
 *   - 1 -> pause (暂停)
 *   - 2 -> skip (跳过)
 *   - 其他 -> other
 */
public class SplitProcessFunction extends ProcessFunction<String, String> {

    // 定义OutputTag
    public static final OutputTag<String> PLAY_TAG = new OutputTag<String>("play"){};
    public static final OutputTag<String> PAUSE_TAG = new OutputTag<String>("pause"){};
    public static final OutputTag<String> SKIP_TAG = new OutputTag<String>("skip"){};
    public static final OutputTag<String> OTHER_TAG = new OutputTag<String>("other"){};
    public static final OutputTag<String> LOGIN_LOCATION_TAG = new OutputTag<>("login_location"){};
    public static final OutputTag<String> UNKNOWN_TAG = new OutputTag<String>("unknown"){};

    @Override
    public void processElement(String jsonStr, Context ctx, Collector<String> out) {
        try {
            // 解析JSON
            JSONObject obj = JSON.parseObject(jsonStr);
            String eventType = obj.getString("event_type");

            // 第一层: 按event_type分流
            if ("MINIK_CLIENT_SONG_PLAY_OPERATE_REQ".equals(eventType)) {
                // 第二层: 按optrate_type分流
                JSONObject eventContent = obj.getJSONObject("event_content");
                Integer oprateType = eventContent.getInteger("optrate_type");

                if (oprateType == null) {
                    ctx.output(OTHER_TAG, jsonStr);
                } else if (oprateType == 0) {
                    ctx.output(PLAY_TAG, jsonStr);      // 播放
                } else if (oprateType == 1) {
                    ctx.output(PAUSE_TAG, jsonStr);     // 暂停
                } else if (oprateType == 2) {
                    ctx.output(SKIP_TAG, jsonStr);      // 跳过
                } else {
                    ctx.output(OTHER_TAG, jsonStr);     // 其他操作
                }
            } else if ("USER_LOGIN_LOCATION_LOG".equals(eventType)) {
                // 分流用户登录信息
                ctx.output(LOGIN_LOCATION_TAG, jsonStr);
            } else {
                // 未知事件类型
                ctx.output(UNKNOWN_TAG, jsonStr);
            }
        } catch (Exception e) {
            // 解析失败,发送到unknown
            ctx.output(UNKNOWN_TAG, jsonStr);
            System.err.println("解析失败: " + jsonStr);
        }
    }
}
