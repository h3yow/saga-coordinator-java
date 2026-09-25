# 本地单进程 Saga 分布式事务协调器

在单个 Spring Boot 进程内提供一套可验证的 Saga 事务协调能力：跨多个参与者的
顺序提交、失败/超时后的逆序补偿、幂等与并发控制、基于本地事件日志的状态
持久化与崩溃恢复。

技术栈沿用骨架工程：Spring Boot 3.4 / Java 21 / JUnit 5；持久化不引入外部
数据库，采用本地目录下的**追加式事件日志**（JSON Lines），因此整套能力可以
在单进程内直接验证崩溃与重启。

---

## 1. 事务状态机

### 1.1 全局状态（`SagaState`）

```
                 所有步骤成功
 STARTED ──▶ RUNNING ───────────────▶ COMPLETED        (终态, 成功)
              │  ▲
              │  │ 重启恢复：重放后处于 RUNNING
              │  └──────────────────────────────┐
              │ 任一步骤 失败/超时/结果未知        │
              ▼                                  │
          COMPENSATING ──全部逆序补偿成功──▶ COMPENSATED        (终态)
              │
              │ 某步骤补偿在 stepTimeout + 重试后仍失败
              ▼
       COMPENSATION_FAILED                                (终态, 需人工介入)

 RUNNING/COMPENSATING ──事务总超时且无法收敛──▶ TIMED_OUT ──▶ MANUAL_INTERVENTION (终态)
 RUNNING（重启后参与者实现缺失）──────────────▶ MANUAL_INTERVENTION              (终态)
```

合法迁移由 `SagaStateMachine` 集中声明；任何不在白名单内的迁移直接抛出
`IllegalTransitionException`，**绝不就地改写状态**。终态包括
`COMPLETED / COMPENSATED / COMPENSATION_FAILED / FAILED / TIMED_OUT /
MANUAL_INTERVENTION`，终态不可再迁移（`TIMED_OUT → MANUAL_INTERVENTION`
为唯一例外，表示超时后仍无法自动收敛）。

### 1.2 步骤状态（`StepState`）

```
PENDING → RUNNING → COMPLETED
                 ↘ FAILED
  COMPENSATING 由 RUNNING/COMPLETED/FAILED 进入（补偿按步骤逆序触发）
  COMPENSATING → COMPENSATED | COMPENSATION_FAILED
```

### 1.3 可区分的失败原因（`FailureReason`）

| 原因 | 含义 | 典型终态 |
| --- | --- | --- |
| `NONE` | 无失败 | `COMPLETED` / `COMPENSATED` |
| `STEP_FAILED` | 参与者正向提交返回业务失败或抛异常 | `COMPENSATED`（补偿成功） |
| `STEP_TIMEOUT` | 单步骤执行超过 `stepTimeout`，或重启后发现崩溃前步骤结果未知、或巡检器判定长时间无进展 | `COMPENSATED` |
| `COMPENSATION_FAILED` | 逆序补偿在首次 + 重试后仍失败 | `COMPENSATION_FAILED` |
| `SAGA_TIMEOUT` | 事务总存活时间超过 `sagaTimeout` | `COMPENSATED` 或 `TIMED_OUT` |
| `STEP_UNKNOWN` | 重启后事件日志引用的参与者实现在注册表中缺失 | `MANUAL_INTERVENTION` |
| `ILLEGAL_TRANSITION` | 触发了状态机不允许的迁移（防御性） | 抛出异常，状态不变 |

每个非成功终态都带 `reason + reasonDetail`（人类可读细节），**不允许静默
丢弃**：补偿失败、补偿无法在期限内收敛、参与者缺失都必须显式标记。

---

## 2. 编排与逆序补偿规则

- 定义：`SagaDefinition(type, steps[], stepTimeout, sagaTimeout, compensationRetries)`。
  步骤按数组顺序正向提交。
- 某步骤 **失败 / 执行超时 / 结果未知** 时：
  1. 记录 `STEP_FAILED` 或 `STEP_TIMED_OUT` 事件；
  2. 记录 `SAGA_COMPENSATION_STARTED` 事件（携带“补偿锚点”=失败步骤下标）；
  3. 从锚点开始**逆序**补偿，直到下标 0。
- **补偿范围包含失败/超时步骤本身**：它的正向效果可能已经发生（结果未知），
  因此也尝试一次补偿；`PENDING` 步骤确定未执行，跳过。
- 每个步骤补偿最多执行 `1 + compensationRetries` 次，每次受 `stepTimeout`
  约束；任一步骤补偿最终失败，事务立刻进入 `COMPENSATION_FAILED`，剩余步骤
  不再补偿，等待人工处理。
- 参与者契约（`SagaStep`）：`execute` 与 `compensate` **必须自身幂等**。
  协调器在超时接管、崩溃恢复时可能以相同 `sagaId` 重复调用；幂等保证重复
  调用只有一次有效业务效果。

### 2.1 并发控制与“不暴露中间态”

- 每个 `sagaId` 一把锁。锁只保护 **状态迁移 + 事件落盘 + 聚合更新**；
  对参与者的实际调用在锁外执行。
- 因此慢/挂死的参与者不会长时间占锁：同事务的超时巡检可以拿锁接管，不同
  事务使用不同锁，互不阻塞。
- 同一 `sagaId` 的重复/并发提交：只有首个调用成为“驱动者”，后来者在锁上
  等待，最终所有调用读取并返回**同一个终结结论**；业务效果只产生一次。
  并发“提交”永远不会返回 `RUNNING` 等中间结论。
- 普通只读 `getStatus` 在参与者锁外执行期间可能观察到一致的 `RUNNING`
  （但绝不会观察到撕裂或非法的状态组合）。
- 锁外等待结束后重新拿锁时，若事务已被巡检器收敛，本次驱动立即放弃，绝不
  覆盖新状态。

---

## 3. 持久化与崩溃恢复

### 3.1 事件溯源

- 事实来源：`<saga.store-dir>/<sagaId>.log`，每行一条 JSON 事件
  （`SagaEvent`：全局序号、sagaId、事件类型、步骤下标、详情、原因、时间戳）。
- 每次状态迁移**先追加事件并写盘，再更新内存聚合**。进程内存中不保存任何
  无法由事件重建的状态。
- 崩溃可能留下最后一行半截 JSON：启动扫描时识别并截断到最后一条完整事件。

事件类型：

| 事件 | 作用 |
| --- | --- |
| `SAGA_CREATED` | 创建事务；detail 内含定义元数据（类型、步骤名、超时、重试次数），用于重启后重建定义 |
| `SAGA_COMPENSATION_STARTED` | 进入补偿态并持久化补偿锚点 |
| `STEP_STARTED / STEP_COMPLETED` | 步骤正向开始/完成 |
| `STEP_FAILED / STEP_TIMED_OUT` | 步骤业务失败 / 超时（含结果未知） |
| `STEP_COMPENSATION_STARTED / STEP_COMPENSATED / STEP_COMPENSATION_FAILED` | 步骤补偿轨迹 |
| `SAGA_FINISHED` | 事务进入终态（detail=终态名，reason=失败原因） |

### 3.2 重启恢复（`recoverAfterRestart`）

1. 扫描日志目录，用 `SagaReplayer` 把每个事务的事件重放为内存聚合；
2. 已终结事务直接装载，查询返回与重启前一致的结论；
3. 未终结事务各自提交到独立线程池继续推进，单事务卡住不阻塞其它事务；
4. 启动超时巡检器。

恢复时的关键判定：

- 重放后当前步骤停在 `RUNNING`（进程在步骤执行中途死亡）——其正向结果
  **未知**，不允许乐观重跑（可能重复生效），而是记录 `STEP_TIMED_OUT`，
  按逆序补偿（参与者补偿幂等，保证最多一次反向效果）。
- 重放后处于 `COMPENSATING`：依据持久化的补偿锚点恢复完整逆序范围；
  已 `COMPENSATED` 的步骤跳过，补偿中途崩溃的步骤重新补偿。
- 事件引用的参与者在新进程 `StepRegistry` 中不存在：标记
  `MANUAL_INTERVENTION`（原因 `STEP_UNKNOWN`），不静默丢弃。

### 3.3 超时恢复（`TimeoutReaper`）

周期性扫描所有非终结事务：

- **总超时**：存活时间 > `sagaTimeout` → 以 `SAGA_TIMEOUT` 触发收敛。
  总超时只负责*触发*补偿；逆序补偿一旦开始就执行完（补偿受 `stepTimeout`
  与重试上限保护），保证“已完成步骤必须被补偿”。
- **无进展超时**：处于 `RUNNING` 且超过 `staleThreshold` 没有状态更新
  （参与者线程挂死、或崩溃后步骤结果未知）→ 以 `STEP_TIMEOUT` 接管，
  判定当前步骤超时并逆序补偿。
- 补偿在重试后仍失败 → `COMPENSATION_FAILED`；总超时后仍无法自动收敛
  → `TIMED_OUT` / `MANUAL_INTERVENTION`。
- 每个事务独立 try/catch，单个事务处理异常只记录日志，不阻塞其它事务。

---

## 4. 如何使用

```java
StepRegistry registry = new StepRegistry();           // 注册参与者实现
SagaCoordinator coordinator = new SagaCoordinator(
        new FileEventLog("build/saga-eventlog"), registry);

SagaDefinition def = SagaDefinition.builder("order")
        .steps(List.of(inventoryStep, paymentStep, shippingStep))
        .stepTimeout(Duration.ofSeconds(5))
        .sagaTimeout(Duration.ofSeconds(30))
        .compensationRetries(1)
        .build();

SagaResult result = coordinator.submit("saga-001", def);  // 同步驱动至结论
// result.state() / reason() / reasonDetail() 给出一致且可解释的结论
coordinator.getStatus("saga-001");                       // 只读查询
coordinator.recoverAfterRestart();                       // 新进程启动时恢复
```

Spring Boot 下 `SagaAutoConfiguration` 已装配单例 `SagaCoordinator`、
`StepRegistry`，并通过 `ApplicationRunner` 在启动时自动执行
`recoverAfterRestart()`；可在 `application.properties` 调整：

```properties
saga.store-dir=build/saga-eventlog
saga.reaper-interval=2s
saga.stale-threshold=10s
```

REST：`GET /api/sagas/{sagaId}` 返回当前结论 JSON。

---

## 5. 验证方法（测试）

运行全部测试：

```bash
mvn test
```

测试位于 `src/test/java/.../tests/saga`，日志中每条轨迹都打印事务标识
`[sagaId]` 与状态/步骤迁移（`状态迁移轨迹`、`步骤轨迹`、`恢复轨迹`、
`巡检轨迹`）。共 13 个用例，覆盖四类场景：

| 测试类 | 覆盖场景 |
| --- | --- |
| `SagaCompletionAndCompensationTest` | ① 全部步骤正常完成；② 参与者失败后逆序补偿（含失败步骤本身）；③ 补偿失败被显式标记为 `COMPENSATION_FAILED` + 可区分原因 |
| `DuplicateAndConcurrentSubmitTest` | ① 重复提交只产生一次业务效果、返回同一结论；② 8 线程并发同一事务被串行化为一次效果、并发提交只拿到终态结论；③ 不同事务互不阻塞 |
| `TimeoutRecoveryTest` | ① 步骤级 `stepTimeout` 触发补偿；② 巡检器接管挂死事务且不阻塞其它事务；③ 事务总超时后已完成步骤被逆序补偿并收敛到明确终态 |
| `RestartRecoveryTest` | ① 已完成事务重启后结论不变；② 正向步骤执行中“崩溃”（detach 协调器+抛弃内存态）后重启，结果未知步骤被判定超时并完整逆序补偿；③ 补偿执行中崩溃后重启，依据补偿锚点恢复完整逆序 |

“重启”的单进程模拟：丢弃旧 `SagaCoordinator`（内存态与线程池），用一个新
协调器指向同一事件日志目录并注册新的参与者实例，再调用
`recoverAfterRestart()`；挂起的步骤用 `CountDownLatch` 门闩模拟进程在
步骤执行中途瞬间消失（旧引擎来不及写任何后续事件）。
