# oceanwatch — 浮标观测快照服务

稳定接收海洋观测平台浮标数据，并支持**快照隔离的游标分页**：分页会话开始时固定一个
接收序号（`snapshotSeq`），此后即使有更早时刻的观测插入，会话也只遍历该快照，
每项恰好返回一次，不重不漏、页序不漂移。

## 构建与运行

```bash
# 构建镜像（构建阶段会先运行全部 Go 单元测试）
docker compose build

# 启动服务（宿主机端口可用环境变量配置，默认 8080）
HOST_PORT=18080 docker compose up -d app
curl -s http://localhost:18080/healthz
```

可配置环境变量：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `HOST_PORT` | `8080` | 宿主机映射端口（compose 层） |
| `PORT` | `8080` | 容器内监听端口 |
| `DB_PATH` | `/data/oceanwatch.db` | SQLite 数据库路径（持久卷） |
| `DEFAULT_PAGE_SIZE` | `50` | 默认页大小 |
| `MAX_PAGE_SIZE` | `200` | 页大小上限 |
| `CURSOR_SECRET` | 未设置时自动生成并持久化到 `$DB_PATH.secret` | 游标签名密钥，重启保持不变 |

## 一次性验收

`verify` 是一个一次性服务：等待**清洁启动**的 app 健康后，插入数据、分页中途追加
更早时刻的数据，并用退出码汇报 **Go 单元测试（镜像构建阶段）+ 镜像构建 + API 冒烟**
的综合结果（0 全部通过，非 0 有失败，日志逐条打印 `PASS/FAIL`）：

```bash
# 冒烟（构建、清洁启动、中途插入早时刻数据、游标错误用例、重启检查点）
docker compose up --build --abort-on-container-exit --exit-code-from verify

# 完整端到端（上面全部 + 重启 app 后续用旧游标，验证游标重启后可用）
./scripts/e2e.sh
```

## API

### `POST /api/streams/{streamId}/samples`

原子接收 1–100 条观测。请求体：

```json
{
  "samples": [
    {"sampleId": "b-0001", "timestamp": "2026-10-05T08:00:00Z", "value": 17},
    {"sampleId": "b-0002", "timestamp": "2026-10-05T08:01:00Z", "value": 19}
  ]
}
```

- 成功 `201`：`{"accepted":2,"currentSeq":7}`
- 批内 `sampleId` 重复，或与已有编号冲突：`409`，**整批拒绝、无部分写入**
  （实现为单事务：表内冲突检查 + 全部插入 + 序号推进一起提交）
- 空批 / 超过 100 条 / 非法 RFC3339 时刻：`400`

### `GET /api/streams/{streamId}/samples`

查询参数：`from`、`to`（RFC3339，`to` 为开区间）、`pageSize`、`cursor`。

- 首次请求把该流**当前接收序号**固定为快照上界，返回体携带不变的 `snapshotSeq`。
- 排序键为 `(timestamp ASC, sampleId ASC)`，keyset 分页，无 OFFSET 漂移。
- 有后续页时返回不透明 `nextCursor`；末页返回 `"nextCursor": null, "done": true`。
- 游标为 HMAC-SHA256 签名的自包含会话（流、时间范围、快照序号、keyset 位置），
  服务重启后仍可继续；下列情况返回明确的 `4xx`：
  - 篡改/伪造：`400 invalid_cursor`
  - 跨流复用：`400 cursor_stream_mismatch`
  - 改变原时间范围：`400 cursor_range_mismatch`（重复相同范围合法）
- 空流也可开会话：`snapshotSeq=0, items=[], done=true`。

```json
{
  "snapshotSeq": 30,
  "items": [
    {"sampleId": "s00", "timestamp": "2026-06-01T00:00:00Z", "value": 0}
  ],
  "nextCursor": "eyJ2Ijox...<签名>",
  "done": false
}
```

### `GET /healthz`

检查数据库连通性，供容器 `HEALTHCHECK` 使用。

## 设计要点

- **原子批量写入**：所有写事务串行（`_txlock=immediate` + 单写令牌），
  在同一 SQLite 事务内完成重复检查、插入与每流单调 `seq` 推进；冲突即回滚。
- **稳定快照**：每条样本入库时分配流内单调递增 `seq`；会话只看
  `seq <= snapshotSeq` 的行。分页中途插入的任何数据（包括更早时刻）对旧会话
  不可见，而新开会话会包含它们，因此每个会话不重不漏。
- **游标**：不透明、签名防篡改；快照定义与位置全部封装其中，服务端无会话状态，
  重启天然可续；密钥可显式配置，默认落盘持久化。
- **持久化**：SQLite（WAL）位于命名卷，重启数据与游标均保留。
