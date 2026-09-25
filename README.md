# Saga Coordinator（本地单进程分布式事务协调器）

基于 Spring Boot 3 / Java 21 的 Saga 事务协调器，**无需外部数据库**，事务状态以
JSON 文件原子落盘，可在本地单进程内验证提交、逆序补偿、幂等并发控制与崩溃恢复。

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

## 2. 事务状态机

```
PENDING ──> RUNNING ──> COMPLETED                 （全部步骤成功，终态）
               │
               ├──> COMPENSATING ──> COMPENSATED            （补偿完成，终态）
               │         └────────> COMPENSATION_FAILED     （补偿无法收敛，终态，待人工处理）
               │
               └──> TIMEOUT_COMPENSATING ──> TIMED_OUT_COMPENSATED   （事务级超时后补偿完成，终态）
                                  └────────> COMPENSATION_FAILED
```

终态（`COMPLETED` / `COMPENSATED` / `COMPENSATION_FAILED` /
`TIMED_OUT_COMPENSATED`）不可再迁移；任何越权迁移都会被拒绝。

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
  转入 `TIMEOUT_COMPENSATING` 并按补偿规则收敛。
- **不阻塞其它事务**：锁按事务 ID 分段（`TransactionLockManager`），巡检逐事务
  try/catch 隔离。
- **恢复前提**：应用启动时需通过 `coordinator.register(definition, contextFactory)`
  注册 Saga 定义与上下文工厂（`submit` 会顺带注册）。

## 5. 本地验证方法

```bash
# 运行全部 14 个单测/集成测试（正常完成、补偿、幂等并发、重启恢复、超时）
mvn clean test

# 启动应用
mvn spring-boot:run

# 查询事务结论与状态迁移轨迹
curl http://localhost:8080/saga/transactions/<txId>
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

每个测试都通过 SLF4J 打印 `tx=<事务标识>` 与完整状态迁移轨迹
（`CREATED / STEP_STARTED / STEP_COMPLETED / STEP_FAILED / COMPENSATION_STARTED /
STEP_COMPENSATED / STATE_CHANGED / RECOVERY_RESUMED / RECOVERY_TIMEOUT ...`），
可用 `mvn test -Dtest=CompensationTest` 查看具体输出。

## 6. 编程用法

```java
SagaDefinition<OrderContext> saga = new SagaDefinition<OrderContext>("order-saga")
        .step("reserve-inventory", OrderContext::reserveInventory, OrderContext::releaseInventory)
        .step("charge-payment",    OrderContext::charge,         OrderContext::refund)
        .step("create-shipment",   OrderContext::ship,           OrderContext::cancelShipment);

// txId 由调用方生成（幂等键）；重复 / 并发提交返回同一个 TransactionResult
TransactionResult result = coordinator.submit(saga, txId, context);

result.getStatus();        // COMPLETED / COMPENSATED / COMPENSATION_FAILED / ...
result.getFailureReason(); // NONE / STEP_FAILED / STEP_TIMEOUT / TRANSACTION_TIMEOUT / COMPENSATION_FAILED
result.getTrace();         // 状态迁移事件轨迹
```

## 7. 配置项（application.properties）

```properties
saga.store-dir=build/saga-store                 # 事务 JSON 文件目录
saga.step-timeout-millis=2000                   # 单步超时，0 表示不限制
saga.transaction-timeout-millis=30000           # 事务级超时（无进展 deadline）
saga.compensation-max-retries=3                 # 补偿最大重试次数
saga.recovery.enabled=true                      # 启动恢复 + 定时巡检开关
saga.recovery.sweep-interval-millis=5000        # 超时巡检周期
```
