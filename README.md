# rt-ai-music-project

基于 Flink 的音乐业务实时数仓项目代码仓库。

当前已经闭环的传统业务主线：

- 歌曲热度统计
- 歌手热度统计
- 地区登录统计
- 实时营收统计

核心技术栈：

- Flink 1.17
- Kafka
- MySQL + Maxwell
- HBase / Phoenix
- Redis
- ClickHouse
- Spring Boot

项目分层：

```text
ODS -> DWD -> DWM -> DWS -> DM -> ClickHouse -> latest view
```

当前代码结构重点：

- `src/main/java/com/music/app/dwd`：DWD 层作业
- `src/main/java/com/music/app/dwm`：DWM 层作业
- `src/main/java/com/music/app/dws`：DWS 层作业
- `src/main/java/com/music/app/dm`：DM 层作业
- `src/main/java/com/music/app/dim`：DIM 层作业
- `src/main/java/com/music/api`：查询接口

当前项目定位：

- 传统实时数仓基础设施已经闭环
- 下一阶段将在此基础上继续增加 AI 音乐助手业务线

说明：

- 这个仓库来源于本地整体工作区中的 `rt-music-project` 模块
- 当前以本地项目演示与迭代为主，不额外做独立工程化拆分
