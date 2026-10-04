# 分布式协调服务（本地可验证）

一个**完全在进程内**运行、用本地测试即可验证的分布式协调服务：用可控逻辑时钟模拟时间、
用方法调用模拟 RPC、用对象生命周期模拟崩溃与恢复，**不依赖任何外部中间件、网络或多线程**。
提供四件能力：

1. 基于多数派的**领导者选举与任期管理**（Raft 风格）；
2. 带**租约与栅栏令牌（Fencing Token）**的分布式锁；
3. **幂等执行与崩溃恢复**（去重表 + 检查点/日志重放）；
4. **历史压缩（状态存档）与滞后节点追赶**（快照 + 增量）。

结论在双主、租约过期、分区恢复、重复提交等场景下保持一致且可解释——每个失败都被
显式归类为一种 `CoordinationErrorCode`，日志中打印任期、栅栏令牌、幂等键与判定依据。

---

## 1. 目录结构

```
core/        SimClock（逻辑时钟）、CoordinationErrorCode（错误归类）、CoordinationException
cluster/     RaftNode（选举/租约/复制/提交/压缩/存档追赶）、RPC 记录（含 InstallSnapshot*）、
             ClusterSimulator（集群/分区/崩溃/新节点加入/离线区段回收模拟）
lock/        LockService、FenceToken、AcquireResult
log/         LogEntry（日志条目）
compaction/  LogCompactor（按阈值把已应用历史整理成 Snapshot 状态存档）
statemachine/KvStateMachine（按位点幂等应用的 K/V 状态机，支持安装存档）
recovery/    DurableStorage、InMemoryDisk（进程内“磁盘”，支持存档存取与冲突截断）、
             Checkpoint、Snapshot（压缩产物：状态 + 幂等去重记录 + 边界位点/任期）
coordination/WriteCoordinator（幂等去重 + 从存档和已应用日志恢复）
config/      CoordinationProperties（coordination.* 配置）
demo/        DemoScenarioRunner（启动时一键演示五场景）
```

---

## 2. 选举与租约模型

### 2.1 选举（多数派 + 单调任期）

- 节点角色：`FOLLOWER / CANDIDATE / LEADER`。
- 选举超时（带每节点确定性扰动，避免同时拉票）未收到合法领导者心跳时，
  候选者 `term + 1`、先投自己、再向其余节点拉票；得到 **多数派（N/2+1）** 票即当选。
- **同一任期一票**：节点在一个 term 内最多投一次，且只投给日志至少和自己一样新的候选者，
  因此**同一任期至多产生一个领导者**，从机制上杜绝双主。
- 任何 RPC 见到更高 `term` 都立即退让为 follower；`term` 只升不降。

### 2.2 领导者租约（Lease）

- 领导者每 `heartbeat-interval-millis` 发一轮心跳（空 AppendEntries）。
- 一轮心跳得到**多数派 ACK**，就把自己的租约续到 `now + lease-ttl-millis`。
- 写请求（`RaftNode.propose`）只在 `role=LEADER` 且 `now <= 租约到期` 时被接受：
  - 被分区隔离、拿不到多数派 ACK → TTL 后无法续约 → 写被拒绝；
  - 收到更高任期 → 立即下台 → 写被拒绝。
- 日志复制同样走多数派：新条目复制到多数派后推进 `commitIndex`，
  且遵循 Raft 安全约束（只直接提交当前 term 的条目），随后按序应用到状态机。

### 2.3 分区恢复为何不会双主 / 旧任期覆盖新任期

- 被隔离的旧领导者在 TTL 后失去租约，不再接受写；多数派一侧会选出**更高 term** 的新领导者。
- 分区恢复后，旧领导者收到新领导者更高 term 的心跳/响应，立即转为 follower 并追上任期，
  不可能再以旧 term 写入。集群层 `ClusterSimulator.leader()` 若同时发现两个
  “有有效租约的领导者”，会直接抛出内部错误（该情况在设计上不应发生）。

---

## 3. 租约锁与栅栏令牌

`LockService` 只在“当前领导者且其租约有效”时颁发锁：

- **栅栏令牌** = `(term, seq, holderId, lockName)`。`seq` 是该任期内的颁发序号，
  令牌比较先看 `term` 再看 `seq`；因此换任期后新锁令牌严格大于旧锁令牌（单调递增）。
- 获取：目标锁无“租约内的持有者”才成功；否则返回 `LOCK_CONTENDED`。
- 续期：令牌匹配且在租约内才成功并把到期时间推后一个 TTL。
- 释放：仅当前持有者（令牌匹配、未换代、未过期）可释放；释放后保留栅栏代次记录。
- **迟到写防线** `validateWriteToken(lock, token, now)`：
  - 锁已被更高令牌持有者拿走 → `STALE_FENCE_TOKEN`；
  - 令牌匹配但 `now > 到期时间` → `LEASE_EXPIRED`；
  - 无持有者记录 / 被他人持有 → `LOCK_CONTENDED`。

三类失败互斥、可区分，测试中逐一断言。

---

## 4. 幂等与恢复语义

### 4.1 恰好一次写入效果

- 写请求带 `idempotencyKey`。`WriteCoordinator.submit` 在提交前先查内存去重表：
  - 未命中 → 调 `RaftNode.propose` 走多数派提交，落日志后记录 `(key → term/index/value)`；
  - 命中 → 直接回放**首次**提交的结论（`dedupHit=true`），不再产生日志。
- 因而**重试、重复提交、同 tick 内的并发交叠提交**，最终都只有一条日志、一次写入；
  重试即使携带不同的值，也以首次提交值为准。

### 4.2 崩溃恢复

- `InMemoryDisk` 扮演节点本地磁盘：`crash(node)` 只丢弃节点的内存对象（RaftNode、
  协调器、状态机），磁盘上的**检查点与日志保留**。
- `restart(node)` 用同一块磁盘重新组装：
  - `RaftNode.recover()` 恢复 `currentTerm / votedFor`，并把已提交日志按序重放到
    一个全新状态机；`KvStateMachine` 以日志位点去重，**重复应用同一位点不产生重复效果**；
  - `WriteCoordinator.restore()` 扫描日志重建幂等去重表，重启后的重试依旧只回放、不二次写入。

### 4.3 回退显式拒绝

下列情况都抛出 `CHECKPOINT_ROLLBACK`，而不是静默继续：

- 检查点 `currentTerm` 变小（任期回退）；
- 检查点 `lastIndex / appliedIndex` 变小（位点回退）；
- 日志非连续追加或新日志 term 小于旧日志 term；
- 状态机被要求应用小于已应用位点的下标。

---

## 5. 历史压缩与滞后节点追赶

### 5.1 压缩触发与保留规则

- **触发（规模阈值）**：某节点在**既有存档之后仍保留的、已经提交并应用**的日志条数达到
  `coordination.compaction-threshold-entries`（默认 8 条）时，在每次应用完新提交后自动压缩；
  设为 `0` 关闭自动压缩。
- **只压已落定生效的内容**：压缩位点恒等于该节点的 `appliedIndex`，
  尚未提交/未确认的条目（如领导者本地已落盘但未获多数派 ACK 的写）**绝不进入存档，也不会被删除**。
- **存档内容**（`Snapshot`）：压缩位点处状态机的**完整 K/V 状态**、逐条来自已应用日志的
  **幂等去重记录**、以及边界位点 `lastIncludedIndex/term`。
- **保留规则**：落盘存档后物理删除严格早于边界位点的日志，**边界条目本身保留**
  （标准 Raft 的 lastIncluded 约定，保证存档之后读取连续）；`firstIndex()` 即保留窗口起点。
- **单调不可逆**：存档位点只能前进，同位点任期必须一致；任何回退/矛盾都以
  `CHECKPOINT_ROLLBACK` 显式拒绝。已承诺生效的写入一条不丢——压缩只是把它们的效果折叠进存档状态。
- **等价性**：恢复固定按“**先安装存档 -> 再从边界位点之后逐位补增量**”执行，
  最终状态、去重结论与从 index=1 完整重放**完全一致**（`CompactionTest` 有专门等价性断言）。

### 5.2 滞后节点追赶（InstallSnapshot + 增量）

- 长时间失联节点或**全新空白节点**（`joinNewNode`）回到集群后，领导者在心跳中发现
  对端 `nextIndex` 已落在自己存档覆盖范围内，就先发 **InstallSnapshot 完整存档**，
  再从 `lastIncludedIndex+1` 逐轮补增量。
- **追赶不阻塞写入**：存档推送复用心跳/写复制的同一轮 RPC，追赶期间集群照常接受并提交新写；
  对端补齐进度后重新进入多数派。
- **不重复计数**：幂等去重记录随存档一起送达，实时应用增量时再按提交位点逐条登记；
  同一个幂等键在“存档 + 增量”中合计只出现一次，重试永远只回放首次结论。
- **冲突处理**：对端与领导者在存档之后的未提交尾巴冲突时，按标准 Raft 截断冲突后缀
  （`truncateSuffixFrom`），不允许把未提交内容当作生效历史。

### 5.3 可恢复窗口边界（超出必须明确挡回）

“掉队太远”不是静默追平，而是显式失败 `BEYOND_RECOVERY_WINDOW`：

- 对端本地历史出现**空洞**（所需位点早于保留窗口、又没有任何存档能覆盖）：
  恢复（`recover`）阶段立刻抛出，绝不允许跳过缺失位点、从某个中间位置接着处理；
- 收到的存档**比本地存档更旧**：拒绝安装，不允许借追赶把状态/位点降级；
- 前置位点落在“存档之后、本地最后位点之前”却无对应条目：同样判超窗。

### 5.4 与既有语义的兼容

原有选举、租约锁、幂等行为在压缩/换主前后保持不变（见 `CompactionSemanticsTest`）：

- 无论压缩发生在请求之前还是之后、期间是否换过领导者，**同一幂等键只产生一次写入效果**，
  新领导者靠“存档去重记录 + 增量应用”重建去重表，换主后重试仍回放首次值；
- **有效租约内的锁持有关系不被压缩中断**（锁状态不参与日志压缩），TTL 内续期、
  栅栏校验照常；过期写仍是 `LEASE_EXPIRED`，旧任期旧令牌写仍是 `STALE_FENCE_TOKEN`，
  竞争失败仍是 `LOCK_CONTENDED`，三者继续可区分；
- 旧领导者恢复后已下台：旧键重试只回放，新键写以 `NOT_LEADER` 拒绝。

---

## 6. 错误归类（`CoordinationErrorCode`）

| 错误码 | 触发场景 |
| --- | --- |
| `NOT_LEADER` | 非领导者节点接受写 / 颁发锁，或写入过程中被更高 term 赶下台 |
| `NO_QUORUM` | 选举或写入无法获得多数派 |
| `LEASE_EXPIRED` | 领导者租约过期后继续写，或锁持有者租约过期后的续期/迟到写 |
| `LOCK_CONTENDED` | 锁仍被其他持有者在有效租约内持有 |
| `STALE_FENCE_TOKEN` | 旧任期/旧代次令牌用于写或释放，锁已被更高令牌持有者拿走 |
| `STALE_TERM` | 请求任期落后于节点当前任期 |
| `CHECKPOINT_ROLLBACK` | 任期/日志位点/应用位点回退、非连续追加、存档位点回退或同位点任期矛盾 |
| `BEYOND_RECOVERY_WINDOW` | 掉队太远：所需历史已被压缩且本地无存档可覆盖（历史空洞），或收到比本地更旧的存档 |

---

## 7. 可配置项

`src/main/resources/application.properties`（均为**逻辑毫秒**，非真实时间）：

| 键 | 默认值 | 含义 |
| --- | --- | --- |
| `coordination.node-count` | `3` | 模拟集群节点数 |
| `coordination.election-timeout-millis` | `100` | 选举超时 |
| `coordination.heartbeat-interval-millis` | `30` | 领导者心跳间隔 |
| `coordination.lease-ttl-millis` | `60` | 领导者/锁租约 TTL |
| `coordination.compaction-threshold-entries` | `8` | 历史压缩阈值：存档之后仍保留的已应用日志达到该条数即压缩成存档；`0`=关闭压缩 |
| `coordination.run-demo-on-startup` | `false` | 启动时是否演示五个场景 |

---

## 8. 本地验证方法

### 8.1 单元测试（推荐）

```bash
mvn test
```

覆盖七组共 26 个用例，全部确定性可复现：

- `LeaderElectionTest`：同任期唯一领导者；隔离领导者→失多数派/租约过期→拒绝写；
  多数派选出更高 term 新领导者；分区恢复无双主、旧任期不覆盖新任期；多轮改选任期单调。
- `LeaseLockTest`：`LOCK_CONTENDED` / `LEASE_EXPIRED` / `STALE_FENCE_TOKEN` 可区分；
  TTL 内续期成功、TTL 后迟到写被拒；新任期令牌严格更大，旧令牌写与释放都被拒；
  follower 不能颁发锁。
- `IdempotencyRecoveryTest`：重试/并发重复提交只有一次写入；崩溃重启从检查点恢复、
  不重复应用；旧任期迟到写在落盘前被拒且不留痕、副本在领导者崩溃后仍保有已提交数据；
  任期/位点/应用位点回退被显式拒绝。
- `CompactionTest`（历史压缩，4 个）：达到可配置阈值才压缩、未达阈值不压缩，
  存档只含已提交生效内容；“存档 + 增量”与从头完整重放结果逐条一致；
  压缩后崩溃重启先装存档再补增量，旧键重试命中存档去重、值不被覆盖；
  存档位点绝不越过已应用位点。
- `CatchUpTest`（滞后追赶，4 个）：空白新节点靠存档+增量补齐并重新参与多数派；
  失联节点恢复期间集群照常写入、同一次写在存档/增量中合计只出现一次、重试均命中去重；
  掉队太远（历史空洞、无存档覆盖）在恢复时即被 `BEYOND_RECOVERY_WINDOW` 明确挡回，
  不假装追平、不从中间位点续传；更旧的存档不能覆盖较新状态。
- `CompactionSemanticsTest`（语义保持，3 个）：跨多轮压缩与换主后同一幂等键仍只一次生效、
  以首次值为准；有效租约锁不被压缩中断、竞争/过期/令牌三类错误仍可区分；
  新任期+压缩后旧栅栏令牌写仍判 `STALE_FENCE_TOKEN`。
- `SmokeTest`：基本选举冒烟。

测试日志会打印 **term、栅栏令牌、幂等键、判定依据**，例如：

```
[scenario-令牌] 旧持有者迟到写被拒 errorCode=STALE_FENCE_TOKEN 旧令牌=FenceToken(term=1,...) 当前令牌=FenceToken(term=2,...) (判定依据: 栅栏代次已前进)
[scenario-幂等] 幂等键=order-pay-1001 首次 index=1 term=1；重试结论 dedupHit=true/true,true (判定依据: 去重表命中即回放)
[scenario-回退] 任期回退被拒 errorCode=CHECKPOINT_ROLLBACK 原因=拒绝任期回退: storedTerm=5, requestedTerm=4
```

### 8.2 启动应用一键演示

```bash
mvn spring-boot:run -Dspring-boot.run.arguments="--coordination.run-demo-on-startup=true"
```

进程启动后会在日志中依次演示：选举与失多数派拒绝写、锁三类失败、分区恢复无双主、
幂等 + 崩溃恢复、历史压缩 + 存档追赶，随后 Web 容器继续运行（`server.port=0` 随机端口）。

---

## 9. 模拟手段与边界说明

- **逻辑时钟**：`SimClock` 只能单调向前推进，所有节点共享；测试用 `cluster.advance(n)`
  精确制造“TTL 到期”“选举超时”，杜绝真实 sleep 的抖动。
- **网络分区**：`isolate(node)` 让该节点与其余节点双向不可达，RPC 返回空（视作超时）；
  `healPartition()` 恢复连通。
- **崩溃**：`crash(node)` 删除节点内存对象、保留 `InMemoryDisk`；`restart(node)` 重建。
- **新节点加入**：`joinNewNode(node)` 以一块空白磁盘让新节点上线，由领导者经
  InstallSnapshot 存档 + 增量补齐。
- **离线区段回收**：`simulateOfflineRetentionGc(node, index)` 模拟节点失联期间本地
  `[1, index]` 区段被保留策略物理回收，用于确定性地制造“超出可恢复窗口”的掉队节点。
- 为保持实现精简，本模拟是**单线程同步**的：选举/复制在一次 tick 或一次写调用内完成，
  不模拟真实网络乱序、磁盘 fsync 延迟与拜占庭故障；但选举安全性、租约失效、
  栅栏令牌与幂等去重的判定逻辑与真实 Raft/租约系统一致。
