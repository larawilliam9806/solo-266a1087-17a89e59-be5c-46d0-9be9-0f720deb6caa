# Buoy Data Lake Compaction Service

单进程浮标数据湖 API：按 UTC 日分区追加上传 CSV（浮标号、UTC 时刻、盐度），
每次上传落为一个 Parquet 文件；支持按日读取全部记录与当前文件数；支持按日
触发后台压实，把启动时已有的若干小文件合并为一份，降低下游扫描的小文件开销。
同时支持按上传响应中的**文件名撤回整批观测**（多集语义），撤回与压实交错
执行时保证读取一致、撤回结果持久且可幂等重复调用。

技术栈：FastAPI + PyArrow + SQLite（WAL），单进程设计。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
# 服务监听 http://localhost:8000 ，交互文档 http://localhost:8000/docs
```

所有状态（Parquet 数据 + SQLite 元数据）都在 `/data` 卷中，容器重启不丢数据。

### 本地运行

```bash
pip install -r requirements.txt
DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 运行测试

```bash
pip install -r requirements-dev.txt
python -m pytest tests/ -q
```

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/days/{day}/upload` | 上传 CSV（multipart 文件或 `text/csv` 原始 body），追加为一个 Parquet；响应含 `file`（撤回句柄）与 `withdrawable` |
| GET | `/days/{day}/records` | 读取当日全部记录（一致快照），含 `file_count`/`row_count` |
| GET | `/days/{day}/files` | 当日活跃文件清单与文件数 |
| POST | `/days/{day}/compactions` | 启动后台压实（202）；当日已有压实进行中返回 409 |
| GET | `/days/{day}/compactions` | 当日压实任务列表 |
| GET | `/days/{day}/compactions/{job_id}` | 查询压实任务状态 |
| POST | `/days/{day}/withdrawals` | 按上传文件名撤回一整批（body `{"file": "..."}`），幂等 |
| GET | `/days/{day}/withdrawals` | 当日撤回记录列表 |
| GET | `/days/{day}/uploads/{file_name}` | 查询某上传批次的状态（`active`/`withdrawn`） |
| GET | `/health` | 健康检查 |

`day` 格式为 `YYYY-MM-DD`（UTC 日期分区）。CSV 列为 `buoy_id, ts_utc, salinity`，
表头可选（无表头时按此顺序解析）；时间戳支持 ISO-8601（`Z` 或时区偏移，
裸时间按 UTC 处理）。

### 示例

```bash
# 上传（multipart 或原始 body 均可）
curl -F "file=@obs.csv" http://localhost:8000/days/2026-11-18/upload
curl -X POST -H "Content-Type: text/csv" --data-binary @obs.csv \
     http://localhost:8000/days/2026-11-18/upload

# 读取全部记录与文件数
curl http://localhost:8000/days/2026-11-18/records

# 启动压实并轮询状态
curl -X POST http://localhost:8000/days/2026-11-18/compactions
curl http://localhost:8000/days/2026-11-18/compactions/1

# 撤回一整批（file 取自上传响应；即使源文件已被压实、回收仍然有效）
curl -X POST http://localhost:8000/days/2026-11-18/withdrawals \
     -H "Content-Type: application/json" \
     -d '{"file": "part-9f3c....parquet"}'
# 重复撤回返回 200 且 status=already_withdrawn，数据不会再变化
```

## 撤回（withdrawal）语义

- **仅覆盖功能启用后的批次**：上传时若撤回功能开启（默认开启），该批次会被
  登记为可撤回批次（响应中 `withdrawable: true`），其行 multiset 持久化到
  SQLite。功能关闭期间或开启之前上传的文件没有批次登记，撤回返回 `409`，
  其行作为「无法归源」的数据始终保留；之后重新开启功能也不会追溯登记。
- **多集（multiset）差集**：撤回移除该批次贡献的每一行（含批次内部重复行）；
  同值若仍由其他批次贡献则保留，无法归源的既有行也保留。判定单元是完整三元组
  `(buoy_id, ts_utc, salinity)`，时间戳以微秒整数持久化，与 Parquet 模式精确往返。
- **按原文件名撤回，压实/回收后依然有效**：压实与撤回重写都会在
  `file_lineage` 表留下血缘边，撤回时沿血缘定位当前承载该批次行的活跃
  Parquet，对其做 multiset 差集重写；源文件被回收不影响撤回句柄。
- **原子切换**：重写产物先 tmp+fsync+rename 落盘，再在**单个 SQLite 事务**里
  「退役目标文件 + 登记重写文件（若差集非空）+ 写撤回记录 + 批次置
  withdrawn」。写盘或登记失败 → 原数据与未撤回状态完整保留，可重试；提交后
  重启不会复活，重复撤回返回 `already_withdrawn`。
- **与压实交错**：撤回期间的读取仍走「快照+引用计数」，一次读取只见撤回前或
  撤回后的完整状态；退役文件等在途读取（以及仍在合并的压实快照）引用归零后才
  回收。撤回发生在压实合并期间时，迟到的压实在切换事务中检测到输入文件已变更，
  任务置 `failed` 且**绝不写回已撤回的行**，随后可重新发起压实。
- **撤回记录**：`GET /days/{day}/withdrawals` 列出当日撤回；`GET
  /days/{day}/uploads/{file_name}` 查询单个批次状态。未知文件名返回 `404`，
  功能启用前上传的文件名返回 `409`。

## 一致性与可靠性设计

- **上传原子性**：Parquet 先写 `*.tmp-*` 临时文件，`fsync` 后 `rename` 为正式
  文件并 `fsync` 目录，再在 SQLite 登记为 `active`。读者永远看不到半成品文件；
  崩溃最多留下临时碎片，启动恢复时清理。
- **读取一致性**：读取在同一把锁内完成「快照当日 active 文件列表 + 引用计数
  +1」，因此一次读取必然对应完整的切换前或切换后状态，不会出现新旧混杂；
  响应中的 `file_count` 与实际读取的文件集合严格一致。
- **压实切换原子性**：合并输出完整落盘（tmp+fsync+rename）后，「退役输入文件
  + 登记输出文件 + 任务置为 done」在**单个 SQLite 事务**中提交。提交前崩溃
  → 原文件全部仍在，任务重启后续跑；提交后崩溃 → 切换已生效。不存在中间态。
- **压实期间可写可读**：合并只处理任务启动时快照的输入集合；期间追加的文件
  不在集合内，切换时原样保留、全程可读，不会被遗漏或误删。上传、读取均不
  等待压实结束。
- **同日只受理一项压实**：SQLite 部分唯一索引
  `WHERE status='running'` 在数据库层面保证，并发请求第二个返回 409，
  进程重启后约束依然有效。
- **失败保留原数据**：合并或落盘失败 → 任务标记 `failed`（带错误信息），
  输入文件保持 `active`，可修复后重新发起压实。
- **中断恢复**：启动时 `recover()` 先清理退役文件、孤儿 Parquet（未登记的
  上传残留、被取代的压实输出）和 tmp 碎片，再把仍处于 `running` 的任务重新
  投入执行（其输入必然仍全部 active，重跑是安全的）。已提交的上传不受影响。
- **旧文件回收**：退役文件仅在在途读取引用计数归零后才 unlink；运行中的压实
  任务快照内的退役文件同样被钉住（其 worker 可能仍在合并这些字节）。回收只针对
  登记表中 `retired` 的文件，`active` 文件（包括压实期间新追加的）永远不会
  被误删。另有后台周期清扫兜底。
- **撤回原子性与可恢复性**：撤回重写与上传一样走 tmp+fsync+rename；切换事务
  失败时已落盘但未登记的重写文件属于孤儿 Parquet，启动恢复时与 tmp 碎片一并
  清扫。`batches`/`batch_rows`/`withdrawals`/`file_lineage` 与文件登记同库，
  成功撤回后重启状态不丢、行不复活。

## 目录布局

```
data/
  lake_meta.db                 # SQLite 元数据（files / compactions / batches /
                               # batch_rows / withdrawals / file_lineage）
  dt=2026-11-18/
    part-<uuid>.parquet        # 上传产生的文件
    compacted-<job>-<id>.parquet  # 压实输出
    rewrite-<batch>-<id>.parquet  # 撤回产生的差集重写文件
```

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATA_DIR` | `./data`（容器内 `/data`） | 数据与元数据目录 |
| `LAKE_GC_INTERVAL_SEC` | `2` | 退役文件周期清扫间隔 |
| `LAKE_COMPACTION_DELAY_SEC` | `0` | 测试/演示钩子：合并前人为延迟，便于观察压实期间的并发读写 |
| `LAKE_WITHDRAWALS_ENABLED` | `1` | 撤回功能开关（`1/on/true` 开，`0/off/false/no` 关）；关闭后新上传不再登记为可撤回批次，已登记批次仍可撤回 |

> 注意：一致性依赖进程内锁 + SQLite 事务，请以单进程运行（不要多 worker
> 水平扩展）。
