package com.music.api.service;

import com.music.api.model.AiFeedbackRequest;
import com.music.api.model.AiFeedbackResponse;
import com.music.api.model.AiOdsEvent;
import com.music.api.model.AiQueryRequest;
import com.music.api.model.AiQueryResponse;
import com.music.api.model.SingerHotView;
import com.music.api.model.SongHotView;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class AiAssistantService {

    private static final DateTimeFormatter EVENT_TIME_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final int DEFAULT_RESULT_LIMIT = 3;
    private static final int MAX_RESULT_LIMIT = 10;
    private static final Pattern LIMIT_PATTERN = Pattern.compile("(?:前|top)?\\s*(\\d{1,2})\\s*(?:首|个|位|名)?");
    private static final Pattern SINGER_RECOMMEND_PATTERN = Pattern.compile(
        "(?:推荐|来几首|来点|想听)(?:几首|一些|点)?([\\p{IsHan}A-Za-z0-9·]{2,12})的(?:经典歌|歌|歌曲)"
    );

    private final AiOdsEventPublisher odsEventPublisher;
    private final HotQueryService hotQueryService;

    public AiAssistantService(
        AiOdsEventPublisher odsEventPublisher,
        HotQueryService hotQueryService) {
        this.odsEventPublisher = odsEventPublisher;
        this.hotQueryService = hotQueryService;
    }

    public AiQueryResponse handleQuery(AiQueryRequest request) {
        long startNanos = System.nanoTime();
        String requestId = UUID.randomUUID().toString();
        String sessionId = request.getSessionId() != null && !request.getSessionId().isBlank()
            ? request.getSessionId()
            : UUID.randomUUID().toString();
        String queryEventTime = now();
        IntentMatch intentMatch = detectIntent(request.getQueryText());

        publishQueryEvent(
            request,
            requestId,
            sessionId,
            queryEventTime,
            intentMatch.intentType,
            intentMatch.intentGroup
        );

        AiQueryResponse response = buildQueryResponse(requestId, sessionId, intentMatch);
        response.setLatencyMs((System.nanoTime() - startNanos) / 1_000_000);

        publishResponseEvent(request, response, now());
        return response;
    }

    public AiFeedbackResponse handleFeedback(AiFeedbackRequest request) {
        AiFeedbackResponse response = new AiFeedbackResponse();
        response.setSuccess(true);

        AiOdsEvent event = new AiOdsEvent();
        event.setEventId(UUID.randomUUID().toString());
        event.setEventType("AI_FEEDBACK");
        event.setEventTime(now());
        event.setRequestId(request.getRequestId());
        event.setSessionId(request.getSessionId());
        event.setUserId(request.getUserId());
        event.setPayload(buildFeedbackPayload(request));
        odsEventPublisher.publish(event);

        return response;
    }

    private void publishQueryEvent(
        AiQueryRequest request,
        String requestId,
        String sessionId,
        String eventTime,
        String intentType,
        String intentGroup) {

        AiOdsEvent event = new AiOdsEvent();
        event.setEventId(UUID.randomUUID().toString());
        event.setEventType("AI_QUERY");
        event.setEventTime(eventTime);
        event.setRequestId(requestId);
        event.setSessionId(sessionId);
        event.setUserId(request.getUserId());

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("queryText", request.getQueryText());
        payload.put("intentType", intentType);
        payload.put("intentGroup", intentGroup);
        payload.put("queryTime", eventTime);
        event.setPayload(payload);

        odsEventPublisher.publish(event);
    }

    private void publishResponseEvent(
        AiQueryRequest request,
        AiQueryResponse response,
        String responseTime) {

        AiOdsEvent event = new AiOdsEvent();
        event.setEventId(UUID.randomUUID().toString());
        event.setEventType("AI_RESPONSE");
        event.setEventTime(responseTime);
        event.setRequestId(response.getRequestId());
        event.setSessionId(response.getSessionId());
        event.setUserId(request.getUserId());

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("intentType", response.getIntentType());
        payload.put("intentGroup", response.getIntentGroup());
        payload.put("answerText", response.getAnswerText());
        payload.put("success", response.isSuccess());
        payload.put("latencyMs", response.getLatencyMs());
        payload.put("responseSource", response.getResponseSource());
        payload.put("modelName", "mock-rule-v1");
        payload.put("promptTemplateId", "tpl_music_001");
        payload.put("resultSongIds", response.getResultSongIds());
        payload.put("resultSingerNames", response.getResultSingerNames());
        payload.put("responseTime", responseTime);
        event.setPayload(payload);

        odsEventPublisher.publish(event);
    }

    private Map<String, Object> buildFeedbackPayload(AiFeedbackRequest request) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("feedbackType", request.getFeedbackType());
        payload.put("feedbackTime", now());
        return payload;
    }

    private AiQueryResponse buildQueryResponse(
        String requestId,
        String sessionId,
        IntentMatch intentMatch) {

        switch (intentMatch.intentType) {
            case "HOT_SONG_QUERY":
                return buildHotSongResponse(requestId, sessionId, intentMatch);
            case "HOT_SINGER_QUERY":
                return buildHotSingerResponse(requestId, sessionId, intentMatch);
            case "SINGER_RECOMMEND":
                return buildUnsupportedResponse(
                    requestId,
                    sessionId,
                    intentMatch.intentType,
                    intentMatch.intentGroup,
                    intentMatch.singerName == null
                        ? "第一版歌手推荐还没接真实推荐源，你可以先问我“现在最火的歌有哪些”或“现在最火的歌手是谁”。"
                        : "第一版歌手推荐还没接真实推荐源。我已经识别到你想听的是“" + intentMatch.singerName
                        + "”，你可以先问我“现在最火的歌有哪些”或“现在最火的歌手是谁”。"
                );
            case "SCENARIO_RECOMMEND":
                return buildUnsupportedResponse(
                    requestId,
                    sessionId,
                    intentMatch.intentType,
                    intentMatch.intentGroup,
                    "第一版场景推荐还没接真实推荐结果，你可以先问我“现在最火的歌有哪些”或“现在最火的歌手是谁”。"
                );
            default:
                return buildUnsupportedResponse(
                    requestId,
                    sessionId,
                    intentMatch.intentType,
                    intentMatch.intentGroup,
                    "这个问题我第一版还不支持。你可以先问我热门歌曲、热门歌手，或者让我给你做推荐类问题的占位识别。"
                );
        }
    }

    private AiQueryResponse buildHotSongResponse(
        String requestId,
        String sessionId,
        IntentMatch intentMatch) {

        try {
            List<SongHotView> songs = hotQueryService.getLatestSongHot(intentMatch.limit);
            if (songs.isEmpty()) {
                return buildUnsupportedResponse(
                    requestId,
                    sessionId,
                    intentMatch.intentType,
                    intentMatch.intentGroup,
                    "当前还没有可用的热门歌曲数据，请先确认 DWS / DM 链路是否已经产出最新结果。"
                );
            }

            AiQueryResponse response = createBaseResponse(
                requestId,
                sessionId,
                intentMatch.intentType,
                intentMatch.intentGroup
            );
            response.setSuccess(true);
            response.setAnswerText(
                "当前最热门的" + songs.size() + "首歌曲是："
                    + songs.stream()
                        .map(song -> "《" + song.getSongName() + "》")
                        .collect(Collectors.joining("、"))
                    + "。"
            );
            response.setResultSongIds(
                songs.stream()
                    .map(SongHotView::getSongId)
                    .collect(Collectors.toList())
            );
            response.setResultSingerNames(Collections.emptyList());
            return response;
        } catch (Exception e) {
            return buildUnsupportedResponse(
                requestId,
                sessionId,
                intentMatch.intentType,
                intentMatch.intentGroup,
                "当前热门歌曲数据查询失败，请稍后再试。"
            );
        }
    }

    private AiQueryResponse buildHotSingerResponse(
        String requestId,
        String sessionId,
        IntentMatch intentMatch) {

        try {
            List<SingerHotView> singers = hotQueryService.getLatestSingerHot(intentMatch.limit);
            if (singers.isEmpty()) {
                return buildUnsupportedResponse(
                    requestId,
                    sessionId,
                    intentMatch.intentType,
                    intentMatch.intentGroup,
                    "当前还没有可用的热门歌手数据，请先确认 DWS / DM 链路是否已经产出最新结果。"
                );
            }

            AiQueryResponse response = createBaseResponse(
                requestId,
                sessionId,
                intentMatch.intentType,
                intentMatch.intentGroup
            );
            response.setSuccess(true);
            response.setAnswerText(
                "当前最热门的" + singers.size() + "位歌手是："
                    + singers.stream()
                        .map(SingerHotView::getSingerInfo)
                        .collect(Collectors.joining("、"))
                    + "。"
            );
            response.setResultSongIds(Collections.emptyList());
            response.setResultSingerNames(
                singers.stream()
                    .map(SingerHotView::getSingerInfo)
                    .collect(Collectors.toList())
            );
            return response;
        } catch (Exception e) {
            return buildUnsupportedResponse(
                requestId,
                sessionId,
                intentMatch.intentType,
                intentMatch.intentGroup,
                "当前热门歌手数据查询失败，请稍后再试。"
            );
        }
    }

    private AiQueryResponse buildUnsupportedResponse(
        String requestId,
        String sessionId,
        String intentType,
        String intentGroup,
        String answerText) {

        AiQueryResponse response = createBaseResponse(requestId, sessionId, intentType, intentGroup);
        response.setSuccess(false);
        response.setAnswerText(answerText);
        response.setResultSongIds(Collections.emptyList());
        response.setResultSingerNames(Collections.emptyList());
        return response;
    }

    private AiQueryResponse createBaseResponse(
        String requestId,
        String sessionId,
        String intentType,
        String intentGroup) {

        AiQueryResponse response = new AiQueryResponse();
        response.setRequestId(requestId);
        response.setSessionId(sessionId);
        response.setIntentType(intentType);
        response.setIntentGroup(intentGroup);
        response.setLatencyMs(0L);
        response.setResponseSource("RULE");
        return response;
    }

    private IntentMatch detectIntent(String queryText) {
        String normalizedQuery = normalizeQueryText(queryText);
        int limit = extractLimit(normalizedQuery);

        if (containsAnyKeyword(
            normalizedQuery,
            "热门歌手", "歌手榜", "最火歌手", "最热歌手", "最火的歌手", "最热的歌手"
        )) {
            return new IntentMatch("HOT_SINGER_QUERY", "DATA_QA", limit, null);
        }
        if (containsAnyKeyword(normalizedQuery, "热门歌曲", "最火的歌", "热歌", "歌曲榜", "热门歌", "最热的歌")) {
            return new IntentMatch("HOT_SONG_QUERY", "DATA_QA", limit, null);
        }
        if (containsAnyKeyword(normalizedQuery, "推荐", "来几首", "经典歌", "好听的歌", "想听", "来点")) {
            String singerName = extractSingerName(normalizedQuery);
            if (singerName != null) {
                return new IntentMatch("SINGER_RECOMMEND", "RECOMMEND", limit, singerName);
            }
            if (containsAnyKeyword(
                normalizedQuery,
                "适合", "通勤", "开车", "运动", "跑步", "学习", "工作",
                "睡前", "睡觉", "放松", "安静", "伤感", "开心", "治愈", "聚会", "旅行"
            )) {
                return new IntentMatch("SCENARIO_RECOMMEND", "RECOMMEND", limit, null);
            }
            return new IntentMatch("SCENARIO_RECOMMEND", "RECOMMEND", limit, null);
        }
        return new IntentMatch("OTHER", "DATA_QA", limit, null);
    }

    private String normalizeQueryText(String queryText) {
        if (queryText == null) {
            return "";
        }
        return queryText.trim().replaceAll("\\s+", "");
    }

    private int extractLimit(String queryText) {
        Matcher matcher = LIMIT_PATTERN.matcher(queryText);
        if (!matcher.find()) {
            return DEFAULT_RESULT_LIMIT;
        }
        int limit = Integer.parseInt(matcher.group(1));
        return Math.min(Math.max(limit, 1), MAX_RESULT_LIMIT);
    }

    private String extractSingerName(String queryText) {
        Matcher matcher = SINGER_RECOMMEND_PATTERN.matcher(queryText);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    private boolean containsAnyKeyword(String text, String... keywords) {
        for (String keyword : keywords) {
            if (text.contains(keyword)) {
                return true;
            }
        }
        return false;
    }

    private String now() {
        return LocalDateTime.now().format(EVENT_TIME_FORMATTER);
    }

    private static class IntentMatch {
        private final String intentType;
        private final String intentGroup;
        private final int limit;
        private final String singerName;

        private IntentMatch(String intentType, String intentGroup, int limit, String singerName) {
            this.intentType = intentType;
            this.intentGroup = intentGroup;
            this.limit = limit;
            this.singerName = singerName;
        }
    }
}
