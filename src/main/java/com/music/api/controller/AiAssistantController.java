package com.music.api.controller;

import com.music.api.model.AiFeedbackRequest;
import com.music.api.model.AiFeedbackResponse;
import com.music.api.model.AiQueryRequest;
import com.music.api.model.AiQueryResponse;
import com.music.api.service.AiAssistantService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/ai-assistant")
public class AiAssistantController {

    private final AiAssistantService aiAssistantService;

    public AiAssistantController(AiAssistantService aiAssistantService) {
        this.aiAssistantService = aiAssistantService;
    }

    @PostMapping("/query")
    public AiQueryResponse query(@RequestBody AiQueryRequest request) {
        validateQueryRequest(request);
        return aiAssistantService.handleQuery(request);
    }

    @PostMapping("/feedback")
    public AiFeedbackResponse feedback(@RequestBody AiFeedbackRequest request) {
        validateFeedbackRequest(request);
        return aiAssistantService.handleFeedback(request);
    }

    @GetMapping("/health")
    public String health() {
        return "ok";
    }

    private void validateQueryRequest(AiQueryRequest request) {
        if (request == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "request body 不能为空");
        }
        if (request.getUserId() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "userId 不能为空");
        }
        if (isBlank(request.getQueryText())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "queryText 不能为空");
        }
    }

    private void validateFeedbackRequest(AiFeedbackRequest request) {
        if (request == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "request body 不能为空");
        }
        if (isBlank(request.getRequestId())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "requestId 不能为空");
        }
        if (isBlank(request.getSessionId())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "sessionId 不能为空");
        }
        if (request.getUserId() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "userId 不能为空");
        }
        if (isBlank(request.getFeedbackType())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "feedbackType 不能为空");
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
