# Saga Coordinator（本地单进程分布式事务协调器）

基于 Spring Boot 3 / Java 21 的 Saga 事务协调器，**无需外部数据库**，事务状态以
JSON 文件原子落盘，可在本地单进程内验证提交、逆序补偿、幂等并发控制与崩溃恢复。
本轮新增运维支撑能力：**事务总览分页查询、人工介入处置、推进并发上限与挂起隔离**。

## 1. 能力总览

| 需求 | 实现方式 |
| --- | --- |
| 跨参与者步骤编排 | `SagaDefinition.step(...)` 按序注册正向动作与补偿动作 |
| 逆序补偿 | 任一步骤失败/超时后，仅对**已完成**步骤按依赖逆序补偿 |
| 补偿失败收敛 | 重试 `saga.compensation-max-retries` 次仍失败 → `COMPENSATION_FAILED`（待人工处理），绝不静默丢弃 |
| 幂等提交 | 事务 ID 即幂等键；文件 `createIfAbsent` + 事务分段锁，重复/并发提交只有一次业务效果 |
| 并发不暴露中间态 | 同一事务全程持锁推进，重复请求最终读到的是同一终态结论 |
| 非法迁移拒绝 | `StateMachine` 集中维护合法迁移表，非法迁移抛 `IllegalTransitionException`，不就地改写 |
| 状态持久化 | 每事务一个 JSON 文件，写临时文件 + 原子 move |
| 崩溃恢复 | 启动时 `RecoveryService.recoverAll()` 恢复所有非终态事务；在途步骤依赖参与者幂等重放 |
| 超时收敛 | 单步超时（`STEP_TIMEOUT`）与事务级超时（`TRANSACTION_TIMEOUT`）区分；定时巡检，逐事务隔离不阻塞其它事务 |
| **事务总览查询** | `GET /saga/transactions` 按状态 / 业务类型筛选、按最近进展时间倒序分页；流式扫描 + 有界堆排序，内存占用与事务总量无关 |
| **人工介入处置** | `POST /saga/transactions/{id}/intervention` 支持 `RETRY_COMPENSATION`（再试一次补偿）与 `MARK_RESOLVED`（标记人工已完结）；重复处置幂等，不该处置的状态拒绝（409） |
| **推进并发上限** | `AdmissionController` 信号量限制同时在推进的事务数（`saga.max-concurrent-transactions`），超限请求排队 `saga.admission-queue-timeout-millis` 后拒绝 |
| **挂起事务隔离** | 锁按事务 ID 分段，挂起事务只占用自己的容量许可；其步骤按既有超时规则收敛后许可自动释放 |

## 2. 事务状态机

```
PENDING ──> RUNNING ──> COMPLETED                 （全部步骤成功，终态）
               │
               ├──> COMPENSATING ──> COMPENSATED            （补偿完成，终态）
               │         └────────> COMPENSATION_FAILED    （补偿无法收敛，待人工处理）
               │                            ├──> COMPENSATING        （人工 RETRY_COMPENSATION）
               │                            └──> MANUALLY_RESOLVED  （人工 MARK_RESOLVED，终态）
               │
               └──> TIMEOUT_COMPENSATING ──> TIMED_OUT_COMPENSATED   （事务级超时后补偿完成，终态）
                                  └────────> COMPENSATION_FAILED
```

终态（`COMPLETED` / `COMPENSATED` / `FAILED` / `MANUALLY_RESOLVED` /
`TIMED_OUT_COMPENSATED`）不可再迁移；`COMPENSATION_FAILED` 不会被自动流程改动，
只允许人工介入迁出。任何越权迁移都会被拒绝。

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
  转入 `TIMEOUT_COMPENSATING` 并按补偿规则收敛。容量占满时巡检跳过本轮、下轮再试，
  不会在巡检线程上堆积。
- **不阻塞其它事务**：锁按事务 ID 分段（`TransactionLockManager`），巡检逐事务
  try/catch 隔离。
- **恢复前提**：应用启动时需通过 `coordinator.register(definition, contextFactory)`
  注册 Saga 定义与上下文工厂（`submit` 会顺带注册）。

## 5. 事务总览查询（新增）

```
GET /saga/transactions?status=COMPENSATION_FAILED&sagaType=order-saga&page=0&size=20
```

- `status`（可选）：按事务状态精确筛选，枚举值见状态机；
- `sagaType`（可选）：按业务类型筛选；
- `page`（默认 0）/ `size`（默认 20，上限 200）：分页参数；
- 返回 `PageResult<TransactionSummary>`：`items`（每条含事务标识、业务类型、当前
  状态结论、失败原因、失败详情、创建时间与**最近一次进展时间** `updatedAt`）、
  `totalElements`、`totalPages`；
- 列表按 `updatedAt` 倒序（最近有进展的排前面），时间相同按事务 ID 升序，
  翻页遍历稳定不重不漏。

实现要点：

- 只读取持久化快照，**不触碰任何事务锁**，查询不会干扰正在推进的事务；
- 存储层逐条流式扫描（`TransactionStore.forEach`），排序只保留大小为
  `offset+size` 的有界堆，**内存占用与事务总量无关**，上千条事务也能稳定分页
  （`TransactionQueryTest.thousandsOfTransactionsPageStably` 用 1200 条验证）。

## 6. 人工介入处置（新增）

```
POST /saga/transactions/{txId}/intervention?action=RETRY_COMPENSATION
POST /saga/transactions/{txId}/intervention?action=MARK_RESOLVED
```

- `RETRY_COMPENSATION`：仅允许 `COMPENSATION_FAILED` 状态。事务迁回
  `COMPENSATING` 并从未收敛的步骤继续补偿：成功则收敛为 `COMPENSATED`，
  仍失败则回到 `COMPENSATION_FAILED`（同一结论，可再次处置）。
- `MARK_RESOLVED`：仅允许 `COMPENSATION_FAILED` 状态。事务迁入终态
  `MANUALLY_RESOLVED`，表示人工已在线下处理完毕。

规则与保证：

- **幂等**：对 `MANUALLY_RESOLVED` 重复 `MARK_RESOLVED` 返回同一结论、不再改写
  状态；补偿仍失败时反复 `RETRY_COMPENSATION` 都收敛到同一 `COMPENSATION_FAILED`
  结论。每次有效处置在事件轨迹中追加 `MANUAL_INTERVENTION` 事件并累计
  `interventionCount`，随事务落盘。
- **拒绝（HTTP 409）**：事务不存在、仍在推进（含锁被在途推进持有，立即拒绝不等待）、
  已到其它终态（`COMPLETED`/`COMPENSATED` 等）、或对 `MANUALLY_RESOLVED`
  再发起 `RETRY_COMPENSATION`。被拒绝的处置不产生任何状态漂移。
- **重启稳定**：处置过程与最终结论全部落盘，重启后结论不漂移。

## 7. 隔离与容量约束（新增）

- **并发上限**：`saga.max-concurrent-transactions`（默认 100，0 表示不限制）限制
  同一时间正在推进（提交 / 恢复 / 人工重试补偿）的事务数量，由
  `AdmissionController` 信号量实现。
- **排队或拒绝**：容量占满时，新请求最多排队
  `saga.admission-queue-timeout-millis`（默认 5000ms）；超时仍拿不到许可则抛出
  `ConcurrencyLimitExceededException`（HTTP 429），**请求不落盘、不产生任何业务
  效果**，调用方可稍后重试。
- **挂起不拖累**：许可按事务持有，个别事务因参与者长时间不返回而挂起时，只占
  用自己的一份容量；其余容量内的事务照常提交与收敛。挂起事务的步骤按既有
  单步超时 / 事务级超时规则收敛（或转 `COMPENSATION_FAILED` 待人工），收敛后
  许可自动释放，不会无限占用。
- **许可获取顺序**：事务分段锁 → 准入许可，全链路顺序一致，无死锁；超时巡检
  拿不到许可时跳过本轮、下轮重试。

## 8. 兼容性说明

- **旧行为**：既有提交、补偿、幂等、恢复、超时语义全部不变（14 个原有用例不
  修改即全部通过）。`SagaCoordinator` 保留原 6 参构造器，默认不启用容量限制。
- **旧数据**：历史事务 JSON 无需迁移。新增字段 `interventionCount` 缺省为 0；
  新增状态 `MANUALLY_RESOLVED` 与事件 `MANUAL_INTERVENTION` 只在新处置时产生；
  `COMPENSATION_FAILED` 在状态机中新增两条**仅人工介入可触发**的出边，自动流程
  行为不变（恢复、巡检仍将其视为终态，不会自动重放）。

## 9. 本地验证方法

```bash
# 运行全部 27 个单测/集成测试
mvn clean test

# 只跑新增能力的测试
mvn test -Dtest=TransactionQueryTest      # 筛选 / 分页 / 1200 条稳定分页 / 查询不干扰推进
mvn test -Dtest=ManualInterventionTest    # 处置幂等、被拒、重启稳定
mvn test -Dtest=ConcurrencyLimitTest      # 超限拒绝 / 排队 / 挂起不拖累 / 超时收敛释放容量

# 启动应用
mvn spring-boot:run

# 事务总览（筛选 + 分页）
curl 'http://localhost:8080/saga/transactions?status=COMPENSATION_FAILED&page=0&size=20'

# 查询单事务结论与状态迁移轨迹
curl http://localhost:8080/saga/transactions/<txId>

# 人工介入：再试一次补偿 / 标记人工已完结
curl -X POST 'http://localhost:8080/saga/transactions/<txId>/intervention?action=RETRY_COMPENSATION'
curl -X POST 'http://localhost:8080/saga/transactions/<txId>/intervention?action=MARK_RESOLVED'
```

### 测试覆盖矩阵

| 测试类 | 场景 |
| --- | --- |
| `HappyPathTest` | 三步全部按序成功，无补偿 |
| `CompensationTest` | 第三步失败后 B→A 逆序补偿；B 补偿持续失败 → `COMPENSATION_FAILED` |
| `StepTimeoutTest` | 单步超时 → `STEP_TIMEOUT` → 已完成步骤补偿 |
| `IdempotencyConcurrencyTest` | 重复提交同结论；8 线程并发提交只产生一次业务效果；非法迁移被拒 |
| `RecoveryTest` | 重启后终态稳定；崩溃于 EXECUTING 后新进程幂等续跑；超时巡检收敛；`COMPENSATION_FAILED` 重启不漂移 |
| `SpringRecoveryIntegrationTest` | Spring Bean 装配、全量恢复与多事务巡检隔离 |
| `TransactionQueryTest`（新增） | 状态/类型筛选；翻页不重不漏且排序稳定；1200 条事务稳定分页；查询不阻塞在途事务 |
| `ManualInterventionTest`（新增） | `MARK_RESOLVED` 幂等与重启稳定；`RETRY_COMPENSATION` 收敛/仍失败同结论；进行中/已完结/不存在的事务处置被拒 |
| `ConcurrencyLimitTest`（新增） | 超限排队后拒绝（429 语义）；容量释放后排队请求继续；挂起事务不拖累其它事务；挂起事务按超时收敛并释放容量；被拒请求零副作用 |

每个测试都通过 SLF4J 打印 `tx=<事务标识>` 与完整状态迁移轨迹
（`CREATED / STEP_STARTED / STEP_COMPLETED / STEP_FAILED / COMPENSATION_STARTED /
STEP_COMPENSATED / STATE_CHANGED / RECOVERY_RESUMED / RECOVERY_TIMEOUT /
MANUAL_INTERVENTION ...`），可用 `mvn test -Dtest=ManualInterventionTest` 查看具体输出。

## 10. 编程用法

```java
SagaDefinition<OrderContext> saga = new SagaDefinition<OrderContext>("order-saga")
        .step("reserve-inventory", OrderContext::reserveInventory, OrderContext::releaseInventory)
        .step("charge-payment",    OrderContext::charge,         OrderContext::refund)
        .step("create-shipment",   OrderContext::ship,           OrderContext::cancelShipment);

// txId 由调用方生成（幂等键）；重复 / 并发提交返回同一个 TransactionResult
TransactionResult result = coordinator.submit(saga, txId, context);

result.getStatus();        // COMPLETED / COMPENSATED / COMPENSATION_FAILED / MANUALLY_RESOLVED / ...
result.getFailureReason(); // NONE / STEP_FAILED / STEP_TIMEOUT / TRANSACTION_TIMEOUT / COMPENSATION_FAILED
result.getTrace();         // 状态迁移事件轨迹

// 运维查询与人工介入
PageResult<TransactionSummary> page = queryService.query(
        TransactionStatus.COMPENSATION_FAILED, "order-saga", 0, 20);
TransactionResult after = coordinator.intervene(txId, InterventionAction.RETRY_COMPENSATION);
```

## 11. 配置项（application.properties）

```properties
saga.store-dir=build/saga-store                 # 事务 JSON 文件目录
saga.step-timeout-millis=2000                   # 单步超时，0 表示不限制
saga.transaction-timeout-millis=30000           # 事务级超时（无进展 deadline）
saga.compensation-max-retries=3                 # 补偿最大重试次数
saga.recovery.enabled=true                      # 启动恢复 + 定时巡检开关
saga.recovery.sweep-interval-millis=5000        # 超时巡检周期
saga.max-concurrent-transactions=100            # 同时在推进的事务上限，0 表示不限制
saga.admission-queue-timeout-millis=5000        # 超限请求排队等待时间，超时拒绝
```
