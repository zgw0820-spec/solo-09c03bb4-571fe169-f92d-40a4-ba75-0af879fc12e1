# 按路径恢复备份子目录 — FastAPI 服务

误删某个子目录、而恢复盘放不下整份备份时，值班员可以只从只读挂载的
`tar.gz` 备份里恢复**一个子目录**。

备份成员路径唯一，支持目录、普通文件和硬链接。硬链接按归档根解析，
允许串联，目标可以位于所选目录之外，也可以比链接本身更晚出现。
恢复结果只包含所选目录；指向同一原件的文件在磁盘上共用一个 inode。

## 快速开始

```bash
docker compose up --build
```

启动后：

* Swagger UI：<http://localhost:8000/docs>
* 示例备份：容器内 `/backups/sample-backup.tar.gz`（只读挂载）
* 恢复结果：宿主 `./restored/<restore-id>/2024/...`

在 Swagger 里执行 `POST /api/restores`：

```json
{ "archive": "sample-backup.tar.gz", "path": "data/2024" }
```

需要限制本次恢复的刷盘流水线时，可一并传入可选字段：

```json
{
  "archive": "sample-backup.tar.gz",
  "path": "data/2024",
  "max_pending_flush_files": 4,
  "max_pending_flush_bytes": 8388608
}
```

两个字段留空时使用环境变量 `RESTORE_MAX_PENDING_FLUSH_FILES`（默认 `16`）与
`RESTORE_MAX_PENDING_FLUSH_BYTES`（默认 `67108864`，即 64 MiB）；两者都必须 ≥ 1，
否则返回 `422`。`docker-compose.yml` 已透传同名环境变量，可在 `.env` 或命令行覆盖：

```bash
RESTORE_MAX_PENDING_FLUSH_FILES=4 RESTORE_MAX_PENDING_FLUSH_BYTES=8388608 docker compose up --build
```

或命令行：

```bash
curl -s -X POST http://localhost:8000/api/restores \
  -H 'Content-Type: application/json' \
  -d '{"archive":"sample-backup.tar.gz","path":"data/2024"}' | jq
```

响应示例（截断）：

```json
{
  "requested_path": "data/2024",
  "restored_path": "/restored/3f0c.../2024",
  "directories": 2,
  "files": 3,
  "hardlinks": 7,
  "bytes_written": 14120,
  "bytes_extracted": 1001968,
  "archive_payload_bytes": 987848,
  "skipped_unsafe": 0,
  "skipped_unsupported": 0
}
```

> 响应字段与之前完全一致；`max_pending_flush_*` 只影响刷盘调度，不改变输出。

> `docker compose` 通过 `RESTORE_UID` / `RESTORE_GID`（默认 `1000`）以宿主用户身份
> 运行，保证写进 `./restored` 的文件属主正确。可按需覆盖：
> `RESTORE_UID=$(id -u) RESTORE_GID=$(id -g) docker compose up --build`。

## 示例备份里的跨目录硬链接

`tools/make_sample_archive.py` 生成 `backups/sample-backup.tar.gz`：

| 成员 | 类型 | 目标 |
|------|------|------|
| `shared/blob.bin` | 普通文件 | — （在链接**之前**出现，位于所选目录**之外**） |
| `data/2024/shared_link` | 硬链接 | `shared/blob.bin` |
| `data/2024/chained_mid` | 硬链接 | `shared/blob.bin` |
| `data/2024/chained_link` | 硬链接 | `data/2024/chained_mid`（串联） |
| `data/2024/three_way_a` | 硬链接 | `shared/blob.bin` |
| `data/2024/three_way_b` | 硬链接 | `data/2024/three_way_a`（链向链接） |
| `data/2024/self_reference` | 硬链接 | `data/2024/inside.txt`（目录内原件） |
| `data/2024/forward_link` | 硬链接 | `shared/forward.bin`（目标**晚于**链接出现） |
| `data/2025/...`、`unrelated.bin` | 普通文件 | 不在所选目录，必须跳过 |

恢复 `data/2024` 后，`shared_link`、`chained_link`、`chained_mid`、
`three_way_a`、`three_way_b` 五个名字的 inode 完全相同；
`self_reference` 与 `inside.txt` 共用 inode。

## 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/health` | 健康检查 |
| `GET` | `/api/archives` | 列出可恢复的 `*.tar.gz` / `*.tgz` |
| `POST` | `/api/restores` | 按 `archive` + `path` 恢复一个子目录 |

状态码：`201` 成功；`400` 归档名/归档格式非法；`404` 归档或子目录不存在；
`409` 所选路径不是目录；`422` 路径非法、链接缺失/成环、归档成员冲突；
`429` 并发恢复数超限；`507` 磁盘不足或其它 I/O 错误。

## 实现要点

* **流式读取**：`tarfile` 的 `r|gz` 模式只顺序读一遍，不 seek，适配只读挂载。
* **只写所选目录需要的数据**：无关成员的负载按块读出后丢弃，绝不落盘。
  实测示例备份：归档负载 987,848 B，实际解出 1,001,968 B（≈1.01×）；
  极端情况下（所选目录只含一个指向归档**末尾**外部文件的链接）为 2.0×，
  仍满足“累计解出不超过完整解压一次的两倍”。
* **硬链接**：全部元数据收齐后按归档根解析，支持串联、反向引用、跨目录。
  指向同一普通文件的成员统一 `os.link` 到同一个已落盘副本，因此共享 inode。
  若原件在所选目录内则直接以它为源；否则在**选择组内**选一个名字落盘数据、
  其余链接到它——恢复结果里不会多出所选目录之外的路径。
* **缺少目标 / 成环**：只对“所选目录依赖的链接”报错（`422`），
  所选目录之外的坏链接不影响恢复。
* **原子发布**：所有数据写进 `<output>/.staging/<token>/<basename>`，
  数据与硬链接全部就绪、`fsync` 后才 `rename` 到
  `<output>/<restore-id>/<basename>`。任何失败都会删除本次的 staging 与
  发布目录，`/restored` 下不会残留半成品。
* **可配置的刷盘流水线**（`fsync` 开启时，默认开启）：每个普通文件在**开始写入前**
  就按**完整大小**占用一份额度，直到它的 `fsync` 成功才释放；写操作由解压线程
  直接完成，`fsync` 交给独立线程池。这样前一个文件还在刷盘时，后一个成员已经
  可以分块解压并写入，等待只发生在额度不足时。两类额度：
  - 待刷文件数上限：`RESTORE_MAX_PENDING_FLUSH_FILES` / 请求字段
    `max_pending_flush_files`（默认 16）；
  - 待刷字节数上限：按文件完整声明大小累加，
    `RESTORE_MAX_PENDING_FLUSH_BYTES` / 请求字段 `max_pending_flush_bytes`（默认 64 MiB）。

  指向同一原件的硬链接只写一遍数据、只占一份额度；跨目录补读的外部原件走同一个
  流水线、受同一上限约束，且每个原件只补读一次。单个文件大于字节上限时不会被拒绝：
  它按块恢复，但必须等流水线空闲后独占额度，写入和刷盘期间其他文件不再开始。
  全部文件 `fsync` 成功后才会 `rename` 发布。写入或刷盘报错后不再开始新的写入，
  等在途的写/刷盘结束、释放句柄后才清理暂存目录，异常原样抛出（API 对 I/O 错误
  返回 `507`），其他并发恢复不受影响。关闭刷盘（`RESTORE_FSYNC=0` 或
  `restore_directory(..., fsync=False)`）时行为与之前相同，直接同步写入。
* **安全**：拒绝绝对路径、`..` 穿越、重复/类型冲突成员；符号链接、设备、
  FIFO 一律不落盘（`skipped_unsupported` 计数），避免经由链接逃逸。

## 本地开发与测试

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt pytest httpx
pytest -q
```

测试覆盖（`tests/test_restore.py`、`tests/test_api.py`）：

* 只恢复所选子目录、跳过兄弟目录与无关大文件；
* 跨目录、串联、链向链接、前向引用、目录内原件的 inode 共享；
* 硬链接缺目标 / 成环 → 报错且清理残留；
* 写到一半失败（模拟磁盘满）→ 清理 staging 与发布目录；
* 刷盘流水线（`tests/test_flush_pipeline.py`）：前一个文件刷盘时后一个已开始写入、
  文件数/字节数上限生效、硬链接与跨目录补读只计一次、超限大文件分块独占额度、
  刷盘/写入失败后停止新写入并清理、失败后其他恢复仍可用、关闭刷盘走原同步路径、
  非法上限（请求字段或环境变量）被拒绝；
* 累计解出字节 ≤ 2×归档负载；
* 路径穿越、绝对路径、符号链接逃逸、重复目录项、隐式父目录；
* API 各状态码与 Swagger 可调用的完整 HTTP 流程。

## 已知限制 / 后续工作

* 仅支持 `tar.gz`/`.tgz`（`tar` 的 `r:*` 形式可扩展，当前明确限定 gzip）。
* 符号链接、设备、FIFO 不恢复；如值班流程需要，应显式加入并做目标校验。
* 硬链接目标若在归档中重复出现且内容不同，会按“路径唯一”前提视为异常报错。
* 当前无鉴权、无恢复任务持久化；生产部署应置于内网或加认证，并按需
  增加过期清理策略（启动时已清理 24 小时前的 staging 残留）。
* 刷盘流水线用线程池实现，单个恢复会占用 1 个解压请求线程加最多 4 个刷盘线程；
  多个恢复同时跑时总线程数随并发上限线性增长，真实高并发场景可考虑独立 I/O
  执行器或 `io_uring`/`libaio` 方案。
