# Saga Coordinator（本地单进程分布式事务协调器）

基于 Spring Boot 3 / Java 21 的 Saga 事务协调器，**无需外部数据库**，事务状态以
JSON 文件原子落盘，可在本地单进程内验证提交、逆序补偿、幂等并发控制、崩溃恢复，
并提供运维所需的事务总览查询、人工介入处置、隔离与并发容量约束。

## 1. 能力总览

| 需求 | 实现方式 |
| --- | --- |
| 跨参与者步骤编排 | `SagaDefinition.step(...)` 按序注册正向动作与补偿动作 |
| 逆序补偿 | 任一步骤失败/超时后，仅对**已完成**步骤按依赖逆序补偿 |
| 补偿失败收敛 | 重试 `saga.compensation-max-retries` 次仍失败 → `COMPENSATION_FAILED`（待人工处理），绝不静默丢弃 |
| 幂等提交 | 事务 ID 即幂等键；文件 create-if-not-exists + 事务分段锁，重复/并发提交只有一次业务效果 |
| 并发不暴露中间态 | 同一事务全程持锁推进，重复请求最终读到的是同一终态结论 |
| 非法迁移拒绝 | `StateMachine` 集中维护合法迁移表，非法迁移抛 `IllegalTransitionException`，不就地改写 |
| 状态持久化 | 每事务一个 JSON 文件，写临时文件 + 原子 move |
| 崩溃恢复 | 启动时 `RecoveryService.recoverAll()` 恢复所有非终态事务；在途步骤依赖参与者幂等重放 |
| 超时收敛 | 单步超时（`STEP_TIMEOUT`）与事务级超时（`TRANSACTION_TIMEOUT`）区分；定时巡检，逐事务隔离不阻塞其它事务 |
| **事务总览查询** | 按状态 / 业务类型筛选，按最近进展时间倒序分页；流式摘要扫描，只读不加锁，上千条也稳定返回 |
| **人工介入处置** | 对 `COMPENSATION_FAILED` 事务可"再试一次补偿"或标记 `MANUALLY_RESOLVED`；重复处置幂等，越权处置被拒 |
| **并发容量约束** | 同时推进事务数受 `saga.max-concurrent-transactions` 约束，满员时 `QUEUE` 排队或 `REJECT` 快速拒绝 |
| **卡死隔离** | 参与者长时间不返回时，其它事务照常提交/收敛；巡检 `tryLock` 跳过被占用事务，单任务限时 |

## 2. 事务状态机

```
PENDING ──> RUNNING ──> COMPLETED                 （全部步骤成功，终态）
               │
               ├──> COMPENSATING ──> COMPENSATED            （补偿完成，终态）
               │         └────────> COMPENSATION_FAILED     （补偿无法收敛，待人工处理）
               │
               └──> TIMEOUT_COMPENSATING ──> TIMED_OUT_COMPENSATED   （事务级超时后补偿完成，终态）
                                  └────────> COMPENSATION_FAILED

# 人工介入（仅对 COMPENSATION_FAILED）
COMPENSATION_FAILED ──> COMPENSATING ──> COMPENSATED / COMPENSATION_FAILED  （人工再试一次补偿）
COMPENSATION_FAILED ──> MANUALLY_RESOLVED                                    （人工确认处理完毕，终态）
```

终态（`COMPLETED` / `COMPENSATED` / `COMPENSATION_FAILED` /
`TIMED_OUT_COMPENSATED` / `MANUALLY_RESOLVED`）不会被自动恢复 / 巡检重放；
其中 `COMPENSATION_FAILED` 是"待人工处理"态，仅接受明确的人工处置动作。
任何越权迁移都会被拒绝。

### 步骤状态机

```
INIT ──> EXECUTING ──> COMPLETED ──> COMPENSATING ──> COMPENSATED
                 ├────> FAILED        （业务失败，该步骤无业务效果，不参与补偿）
                 └────> TIMED_OUT     （单步超时，结果未知，不参与补偿）
```

### 失败原因（FailureReason，可区分）

- `STEP_FAILED`：参与者业务抛异常 → 逆序补偿 → `COMPENSATED`
- `STEP_TIMEOUT`：单步超过 `saga.step-timeout-millis` → 逆序补偿 → `COMPENSATED`
- `TRANSACTION_TIMEOUT`：超过事务 deadline 仍无进展 → `TIMEOUT_COMPENSATING` → `TIMED_OUT_COMPENSATED`
- `COMPENSATION_FAILED`：补偿重试达到上限仍失败 → `COMPENSATION_FAILED`，需人工介入
- `MANUALLY_RESOLVED`：运维确认残留效果已人工处理完毕（终态 `MANUALLY_RESOLVED`）
- `NONE`：正常完成

## 3. 补偿规则

1. 失败/超时发生时，只有状态为 `COMPLETED` 的步骤需要补偿；`FAILED`/`TIMED_OUT`/`INIT`
   的步骤没有可确认的业务效果，一律跳过，避免错误补偿。
2. 严格按 **currentStepIndex-1 → 0** 的依赖逆序执行。
3. 每个补偿最多重试 `saga.compensation-max-retries` 次（默认 3）。
4. 任一步补偿无法收敛：事务立即标记 `COMPENSATION_FAILED`，`failureDetail` 记录
   具体步骤与最后一次错误，后续步骤不再补偿，等待人工处理。
5. 参与者的 `action` 与 `compensation` **都必须实现幂等**：崩溃恢复时协调器会对
   "状态为 EXECUTING、结果未知"的步骤重新调用。

## 4. 超时与崩溃恢复规则

- **写入时机**：步骤进入 `EXECUTING` 前先落盘。崩溃可能发生在"已落盘 EXECUTING、
  业务结果未知"窗口；重启后该步骤按幂等方式重放。
- **启动恢复**：`RecoveryScheduler` 监听 `ApplicationReadyEvent`，扫描存储目录，
  对每个非终态事务独立加锁、独立 `recover`，单个事务异常不影响其它事务。
- **定时巡检**：`@Scheduled`（默认 5s）扫描 `deadlineAt < now` 的非终态事务，
  转入 `TIMEOUT_COMPENSATING` 并按补偿规则收敛。
- **不阻塞其它事务**：锁按事务 ID 分段（`TransactionLockManager`），超时巡检对每个事务
  `tryLock`（200ms），拿不到锁说明该事务正被提交线程等其它路径驱动，**本轮直接跳过**，
  绝不在锁上等待；恢复 / 巡检任务跑在独立的 `saga-recovery` 小线程池（4 线程）上并行执行，
  单任务 30s 上限，超时取消。因此某个参与者调用长时间不返回、事务挂住时，既不会拖住
  其它事务的提交与收敛，也不会让巡检线程整体卡死。
- **卡死事务仍会收敛**：单步 `saga.step-timeout-millis > 0` 时，挂住的参与者调用到时即被
  取消并按 `STEP_TIMEOUT` 补偿；若调用本身不可中断，则事务级 deadline 到期后由巡检转
  `TIMEOUT_COMPENSATING` 收敛，不会无限挂着。
- **恢复前提**：应用启动时需通过 `coordinator.register(definition, contextFactory)`
  注册 Saga 定义与上下文工厂（`submit` 会顺带注册）。

## 4.1 事务总览查询（运维）

按**状态**、**业务类型（sagaType）**筛选并分页，按最近进展时间（`updatedAt`）倒序、
事务 ID 升序兜底排序，因此在没有并发写入时分页稳定、不重不漏。列表每行只含摘要
（`TransactionSummary`：事务 ID、类型、当前状态、失败原因、失败详情、创建时间、
最近进展时间），不含步骤明细与完整事件轨迹；需要轨迹时再查单事务详情。

- 存储侧 `FileTransactionStore.scanSummaries` 用 Jackson streaming parser **逐文件、
  逐字段**读取，遇到 `steps` / `events` 数组整体跳过；任意时刻内存里只持有一条摘要，
  随后在服务层过滤排序，事务攒到上千条也不会因一次查询顶高内存。
- 查询**只读、不获取任何事务锁**，与正在推进的事务完全并行，互不干扰。
- 单页大小默认 20，上限 200（`TransactionFilter.MAX_PAGE_SIZE`），超出被钳制。

## 4.2 人工介入处置

对自动收敛不了、停在 `COMPENSATION_FAILED` 的事务，提供两种处置（均为**同步返回最终
结论**，过程与结论落盘，重启后不漂移）：

| 动作 | API / 方法 | 允许的前置状态 | 语义与幂等 |
| --- | --- | --- | --- |
| 再试一次补偿 | `POST /saga/transactions/{id}/manual-retry` / `coordinator.retryCompensation(id)` | `COMPENSATION_FAILED` | 回到 `COMPENSATING` 重新跑逆序补偿；成功 → `COMPENSATED`，仍失败 → `COMPENSATION_FAILED`，可再次发起。对已 `COMPENSATED` / `MANUALLY_RESOLVED` 的事务重复调用是**幂等 no-op**，返回同一结论、无业务效果 |
| 标记人工处理完毕 | `POST /saga/transactions/{id}/manual-resolve`（可选 body `{"note":"..."}`）/ `coordinator.resolveManually(id, note)` | `COMPENSATION_FAILED` | → 终态 `MANUALLY_RESOLVED`，记录处置说明与 `MANUAL_RETRY_REQUESTED` / `MANUALLY_RESOLVED` 事件；重复标记幂等 |

其它状态一律拒绝：对运行中、已正常完成、已超时补偿完成等事务发起处置会抛
`ManualActionRejectedException`（HTTP 409）；事务不存在返回 400。
`COMPENSATION_FAILED` / `MANUALLY_RESOLVED` 永远不会被启动恢复或超时巡检自动重放。

## 4.3 并发容量约束

每个事务从开始驱动到终态持有一个容量许可（`ConcurrencyLimiter`，公平信号量）：

- `saga.max-concurrent-transactions`（默认 50，`0` 表示不限制）限定**同时推进中**的事务数；
- 满员时由 `saga.capacity-policy` 决定：
  - `QUEUE`（默认）：排队等待许可，超过 `saga.capacity-acquire-timeout-millis`（默认 10s）
    仍拿不到则抛 `CapacityExceededException`（HTTP 429），调用方可稍后重试；
  - `REJECT`：不等待，立即抛 `CapacityExceededException`，不占用线程、不产生任何业务效果；
- 许可在事务分段锁**之外**申请，排队 / 拒绝都不会持有任何事务锁；
- 重复提交已结束事务只会短暂取放许可后返回既有结论，不产生业务效果。

> 注意：若把 `saga.step-timeout-millis` 设为 `0`（单步不超时）且参与者调用永久不可中断，
> 该事务会长期占用一个许可直到事务级 deadline 被巡检收敛。生产环境建议保留有限的单步超时。

## 4.4 HTTP API

| 方法与路径 | 说明 |
| --- | --- |
| `GET /saga/transactions?status=&sagaType=&page=0&size=20` | 总览筛选分页 |
| `GET /saga/transactions/{id}` | 单事务结论 + 完整状态迁移轨迹 |
| `POST /saga/transactions/{id}/recover` | 手动触发恢复 / 继续推进 |
| `POST /saga/transactions/{id}/manual-retry` | 人工再试一次补偿 |
| `POST /saga/transactions/{id}/manual-resolve` | 标记人工处理完毕（可选 `{"note":"..."}`） |

错误响应：`429 CAPACITY_EXCEEDED`、`409 MANUAL_ACTION_REJECTED`、`400 BAD_REQUEST`，
响应体统一为 `{"error": "...", "message": "..."}`。

## 5. 本地验证方法

```bash
# 运行全部 29 个单测/集成测试
mvn clean test

# 只跑本轮新增的三类运维测试
mvn test -Dtest=TransactionQueryTest
mvn test -Dtest=ManualInterventionTest
mvn test -Dtest=IsolationConcurrencyTest
mvn test -Dtest=SagaWebApiIntegrationTest

# 启动应用
mvn spring-boot:run

# 事务总览：按状态 / 业务类型筛选分页（最近进展倒序）
curl 'http://localhost:8080/saga/transactions?status=COMPENSATION_FAILED&page=0&size=20'

# 查询事务结论与状态迁移轨迹
curl http://localhost:8080/saga/transactions/<txId>

# 人工再试一次补偿 / 标记人工处理完毕
curl -X POST http://localhost:8080/saga/transactions/<txId>/manual-retry
curl -X POST http://localhost:8080/saga/transactions/<txId>/manual-resolve \
     -H 'Content-Type: application/json' -d '{"note":"已手工对账冲正"}'
```

### 与旧行为、旧数据的兼容范围

- **旧 JSON 数据可直接读取**：新增的 `manualRetryCount` / `manualResolutionNote` 字段
  在旧文件中缺失时反序列化取默认值（0 / null）；新增枚举值（`MANUALLY_RESOLVED` 状态、
  `MANUALLY_RESOLVED` 失败原因、两类人工事件）只在发生对应动作后出现，旧数据状态不变。
- **旧提交行为不变**：`SagaCoordinator` 保留原 6 参构造器（默认不限并发），
  `submit / recover / timeoutTransaction` 签名与结论保持兼容。
- **终态语义的唯一变化**：`COMPENSATION_FAILED` 现在额外接受人工动作
  （可转 `COMPENSATING` 重试、或转 `MANUALLY_RESOLVED`）；它对自动恢复 / 巡检仍然是终态，
  原有"重启不漂移、不自动重放"的用例与结论不受影响。
- 容量限制与新的隔离线程池是**新增约束**：默认上限 50、`QUEUE` 策略，对原先的低并发
  本地验证透明；若需要旧的"无上限"行为，设置 `saga.max-concurrent-transactions=0`。

### 测试覆盖矩阵

| 测试类 | 场景 |
| --- | --- |
| `HappyPathTest` | 三步全部按序成功，无补偿 |
| `CompensationTest` | 第三步失败后 B→A 逆序补偿；B 补偿持续失败 → `COMPENSATION_FAILED` |
| `StepTimeoutTest` | 单步超时 → `STEP_TIMEOUT` → 已完成步骤补偿 |
| `IdempotencyConcurrencyTest` | 重复提交同结论；8 线程并发提交只产生一次业务效果；非法迁移被拒 |
| `RecoveryTest` | 重启后终态稳定；崩溃于 EXECUTING 后新进程幂等续跑；超时巡检收敛；`COMPENSATION_FAILED` 重启不漂移 |
| `SpringRecoveryIntegrationTest` | Spring Bean 装配、全量恢复与多事务巡检隔离 |
| `TransactionQueryTest` | 按状态/类型筛选；同时间戳下 ID 兜底的稳定分页；1200 条事务分页与页大小钳制 |
| `ManualInterventionTest` | 人工重试补偿收敛且幂等；错误状态拒绝；人工标记幂等且重启不漂移 |
| `IsolationConcurrencyTest` | 卡死事务不拖累健康事务且自身按时超时；REJECT 立即拒绝；QUEUE 排队等空位；巡检跳过被占锁事务仍收敛其它事务；查询不阻塞在途事务 |
| `SagaWebApiIntegrationTest` | 列表端点筛选分页；人工处置 200/409/400 语义；人工标记端到端稳定 |

每个测试都通过 SLF4J 打印 `tx=<事务标识>` 与完整状态迁移轨迹
（`CREATED / STEP_STARTED / STEP_COMPLETED / STEP_FAILED / STEP_TIMED_OUT /
COMPENSATION_STARTED / STEP_COMPENSATED / STATE_CHANGED / RECOVERY_RESUMED /
RECOVERY_TIMEOUT / MANUAL_RETRY_REQUESTED / MANUALLY_RESOLVED ...`），可用
`mvn test -Dtest=IsolationConcurrencyTest` 查看具体输出。

## 6. 编程用法

```java
SagaDefinition<OrderContext> saga = new SagaDefinition<OrderContext>("order-saga")
        .step("reserve-inventory", OrderContext::reserveInventory, OrderContext::releaseInventory)
        .step("charge-payment",    OrderContext::charge,         OrderContext::refund)
        .step("create-shipment",   OrderContext::ship,           OrderContext::cancelShipment);

// txId 由调用方生成（幂等键）；重复 / 并发提交返回同一个 TransactionResult
TransactionResult result = coordinator.submit(saga, txId, context);

result.getStatus();        // COMPLETED / COMPENSATED / COMPENSATION_FAILED / MANUALLY_RESOLVED / ...
result.getFailureReason(); // NONE / STEP_FAILED / STEP_TIMEOUT / TRANSACTION_TIMEOUT / COMPENSATION_FAILED / MANUALLY_RESOLVED
result.getTrace();         // 状态迁移事件轨迹

// 运维：总览筛选分页（只读、不加事务锁）
PageResult<TransactionSummary> page =
        queryService.query(new TransactionFilter(TransactionStatus.COMPENSATION_FAILED,
                "order-saga", /*page*/ 0, /*size*/ 20));

// 运维：人工介入（仅对 COMPENSATION_FAILED；重复调用幂等，越权状态抛 ManualActionRejectedException）
coordinator.retryCompensation(txId);          // 再试一次补偿
coordinator.resolveManually(txId, "已手工对账"); // 标记人工处理完毕 → MANUALLY_RESOLVED
```

## 7. 配置项（application.properties）

```properties
saga.store-dir=build/saga-store                 # 事务 JSON 文件目录
saga.step-timeout-millis=2000                   # 单步超时，0 表示不限制
saga.transaction-timeout-millis=30000           # 事务级超时（无进展 deadline）
saga.compensation-max-retries=3                 # 补偿最大重试次数
saga.max-concurrent-transactions=50             # 同时推进中的事务上限，0 表示不限制
saga.capacity-policy=QUEUE                      # 容量满时：QUEUE 排队 / REJECT 快速拒绝
saga.capacity-acquire-timeout-millis=10000      # QUEUE 排队等待上限，超时拒绝（429）
saga.recovery.enabled=true                      # 启动恢复 + 定时巡检开关
saga.recovery.sweep-interval-millis=5000        # 超时巡检周期
```
