# RocketMQ gRPC Benchmark

基于 Apache RocketMQ gRPC Java SDK 的独立压测工具，支持普通、FIFO、延迟、事务消息，以及 PushConsumer 和 SimpleConsumer。

## 前置条件

- JDK 8+、Maven 3.6+
- RocketMQ 5.x，并启用 Proxy gRPC endpoint（例如 `host:8081`，不是 NameServer `9876`）
- Topic 和 Consumer Group 已预先创建；Topic 类型必须与测试类型一致

## 构建

```bash
bin/build.sh
bin/test.sh
```

生成可运行的 `target/rocketmq-grpc-benchmark-all.jar`。运行参数可通过 `JAVA_OPTS` 传入 JVM，例如：

```bash
JAVA_OPTS="-Xms2g -Xmx2g -Drocketmq.log.level=WARN" bin/producer.sh --help
```

## Producer

普通消息同步发送：

```bash
bin/producer.sh \
  --endpoints localhost:8081 --ssl false \
  --topic benchmark-normal --topic-type normal \
  --send-mode sync --threads 64 --message-size 1024 \
  --warmup-seconds 10 --duration-seconds 60 --csv normal.csv
```

普通、FIFO 和延迟消息可使用 `--send-mode async --max-inflight 4096`。FIFO 和延迟示例：

```bash
bin/producer.sh --endpoints localhost:8081 --ssl false \
  -t benchmark-fifo --topic-type fifo --message-groups 128 --send-mode async

bin/producer.sh --endpoints localhost:8081 --ssl false \
  -t benchmark-delay --topic-type delay --delay-ms 10000 --send-mode async
```

事务消息只支持同步发送；以下测试约 90% 提交、10% 回滚。事务回查会读取消息中保存的原决定：

```bash
bin/producer.sh --endpoints localhost:8081 --ssl false \
  -t benchmark-tx --topic-type tx --commit-percent 90 --threads 32
```

常用生产参数：

- `--rate 0`：全局发送速率上限，0 为不限速
- `--trace-enabled true|false`：默认 `true`，为每条消息写入唯一 `messageKey`、递增序号与发送时间（见下文）
- `--key-enabled`：仅在 `--trace-enabled false` 时单独开启 messageKey
- `--tag TAG`、`--property-size N`
- `--max-attempts N`：SDK 内部最大尝试次数；事务发送不执行 SDK 内部重试
- `--journal PATH`：追加写入每条消息的发送证据，供离线对账
- 默认持续运行；`--duration-seconds N` 设置预热后的测量时长，Ctrl-C/SIGTERM 可优雅退出

## PushConsumer

```bash
bin/push-consumer.sh \
  --endpoints localhost:8081 --ssl false \
  --topic benchmark-normal --consumer-group benchmark-push \
  --threads 32 --max-cache-messages 8192 --max-cache-bytes 134217728 \
  --filter-type tag --filter-expression '*' --csv push.csv \
  --journal push.journal
```

FIFO Topic 可按需添加 `--fifo-accelerator`。该选项提高同一客户端内不同 message group 的并行度，也可能提高重复消费概率。

## SimpleConsumer

```bash
bin/simple-consumer.sh \
  --endpoints localhost:8081 --ssl false \
  --topic benchmark-normal --consumer-group benchmark-simple \
  --threads 8 --batch-size 16 --await-ms 30000 --invisible-ms 15000 \
  --filter-type tag --filter-expression '*' --csv simple.csv \
  --journal simple.journal
```

SimpleConsumer 使用一个线程安全客户端和多个 receive/ACK 工作线程。只有 ACK 成功的消息计入成功数；`recv` 在 ACK 前写入 journal，`done` 在 ACK 成功后写入。

## 消息追踪与本地核对

默认 `--trace-enabled true` 时，Producer 为每条消息写入：

| 字段 | 位置 | 说明 |
|------|------|------|
| messageKey | `Message.keys` | `benchmark-{instanceId}-{seq}`，跨进程唯一 |
| `benchmarkSeq` | property | 进程内递增序号 |
| `benchmarkSendTs` | property | 客户端发送前的毫秒时间戳 |

Consumer 侧：

- 按 messageKey（缺失时回退 messageId）做**本进程**去重，输出 `dup` / `unique`
- 端到端延迟优先用 `benchmarkSendTs`，否则回退 `bornTimestamp`
- 可选 `--journal` 落盘：Producer 写 `sent`；Consumer 写 `recv`（收到）与 `done`（业务/ACK 完成）

带 journal 的对测示例：

```bash
bin/producer.sh --endpoints localhost:8081 --ssl false \
  -t chaos-normal --duration-seconds 60 \
  --journal /tmp/chaos-sent.journal

bin/push-consumer.sh --endpoints localhost:8081 --ssl false \
  -t chaos-normal -g chaos-cg \
  --journal /tmp/chaos-consume.journal
```

Journal 行为文本，首行为元数据注释，数据列为：

`timestamp,event,key,seq,sendTs,messageId,statusOrExtraTs,duplicate`

- Producer `sent`：`statusOrExtraTs` 为 `ok`/`fail`
- Consumer `recv`：`statusOrExtraTs` 为收到时间戳；`done` 为完成时间戳，`duplicate` 为 `dup`/`unique`

客户端可直接给出：发送成功数、本机消费数、本机重复数、基于 sendTs 的 E2E。  
**跨进程最终丢失数 / 跨实例重复数**需用两侧 journal 离线 diff，不在本工具进程内计算。

## ACL、TLS 和 Namespace

参数优先于环境变量：

```bash
export ROCKETMQ_ENDPOINTS=proxy.example.com:8080
export ROCKETMQ_ACCESS_KEY=your-access-key
export ROCKETMQ_SECRET_KEY=your-secret-key
export ROCKETMQ_SECURITY_TOKEN=optional-token

bin/producer.sh -t benchmark-normal --namespace tenant-a --ssl true
```

也可使用 `--access-key`、`--secret-key`、`--security-token`；生产环境更建议环境变量，避免凭证出现在进程参数中。

## 输出口径

控制台和可选 CSV 每个周期输出 TPS、成功/失败数、本机重复数（`dup`/`unique`）、MB/s、操作延迟平均值和 P50/P95/P99/最大值。Consumer 还输出端到端延迟（优先 `benchmarkSendTs`）；延迟消息额外输出相对预定投递时间的 delivery lag。

跨机器端到端延迟要求机器时钟同步。检测到负延迟时，该样本不进入延迟分位数并计入 `clockSkewSamples`。

查看完整参数：

```bash
bin/producer.sh --help
bin/push-consumer.sh --help
bin/simple-consumer.sh --help
```

## Docker

```bash
docker build -t rocketmq-grpc-benchmark .

docker run --rm rocketmq-grpc-benchmark producer \
  --endpoints host.docker.internal:8081 --ssl false \
  --topic benchmark-normal --duration-seconds 60

docker run --rm -v "$PWD:/data" rocketmq-grpc-benchmark producer \
  --endpoints host.docker.internal:8081 --ssl false \
  --topic benchmark-normal --duration-seconds 60 \
  --journal /data/sent.journal
```

Linux 上连接宿主机 Proxy 时，可使用宿主机实际地址或添加
`--add-host host.docker.internal:host-gateway`。ACL 参数建议通过 `-e ROCKETMQ_ACCESS_KEY=...`
等环境变量传入。镜像默认以非 root 用户运行；写入 journal 时请挂载可写目录。
