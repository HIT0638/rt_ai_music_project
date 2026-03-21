package com.music.api.service;

import com.music.api.model.SingerHotView;
import com.music.api.model.SongHotView;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Service
public class HotQueryService {

    private static final DateTimeFormatter FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final JdbcTemplate jdbcTemplate;

    public HotQueryService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<SongHotView> getLatestSongHot(int limit) {
        String sql =
            "SELECT window_start, window_end, song_id, song_name, play_count " +
            "FROM rt_music.dm_song_hot_10s " +
            "WHERE window_end = (SELECT max(window_end) FROM rt_music.dm_song_hot_10s) " +
            "ORDER BY play_count DESC " +
            "LIMIT ?";

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            SongHotView view = new SongHotView();
            view.setWindowStart(formatTimestamp(rs.getTimestamp("window_start")));
            view.setWindowEnd(formatTimestamp(rs.getTimestamp("window_end")));
            view.setSongId(rs.getString("song_id"));
            view.setSongName(rs.getString("song_name"));
            view.setPlayCount(rs.getLong("play_count"));
            return view;
        }, limit);
    }

    public List<SingerHotView> getLatestSingerHot(int limit) {
        String sql =
            "SELECT window_start, window_end, singer_info, play_count " +
            "FROM rt_music.dm_singer_hot_10s " +
            "WHERE window_end = (SELECT max(window_end) FROM rt_music.dm_singer_hot_10s) " +
            "ORDER BY play_count DESC " +
            "LIMIT ?";

        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            SingerHotView view = new SingerHotView();
            view.setWindowStart(formatTimestamp(rs.getTimestamp("window_start")));
            view.setWindowEnd(formatTimestamp(rs.getTimestamp("window_end")));
            view.setSingerInfo(rs.getString("singer_info"));
            view.setPlayCount(rs.getLong("play_count"));
            return view;
        }, limit);
    }

    private String formatTimestamp(Timestamp timestamp) {
        return timestamp.toLocalDateTime().format(FORMATTER);
    }
}
