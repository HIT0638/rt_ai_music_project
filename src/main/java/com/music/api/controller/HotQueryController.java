package com.music.api.controller;

import com.music.api.model.SingerHotView;
import com.music.api.model.SongHotView;
import com.music.api.service.HotQueryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/hot")
public class HotQueryController {

    private final HotQueryService hotQueryService;

    public HotQueryController(HotQueryService hotQueryService) {
        this.hotQueryService = hotQueryService;
    }

    @GetMapping("/songs/latest")
    public List<SongHotView> latestSongHot(
        @RequestParam(defaultValue = "10") int limit) {
        return hotQueryService.getLatestSongHot(limit);
    }

    @GetMapping("/singers/latest")
    public List<SingerHotView> latestSingerHot(
        @RequestParam(defaultValue = "10") int limit) {
        return hotQueryService.getLatestSingerHot(limit);
    }

    @GetMapping("/health")
    public String health() {
        return "ok";
    }
}
