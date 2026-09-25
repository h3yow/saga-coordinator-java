package com.github.h3yow.sagacoordinatorjava.saga.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.sagacoordinatorjava.saga.model.FailureReason;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaDefinition;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaState;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaStep;
import com.github.h3yow.sagacoordinatorjava.saga.model.StepState;
import com.github.h3yow.sagacoordinatorjava.saga.model.StepOutcome;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaContext;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaException;
import com.github.h3yow.sagacoordinatorjava.saga.store.EventLog;
import com.github.h3yow.sagacoordinatorjava.saga.store.EventType;
import com.github.h3yow.sagacoordinatorjava.saga.store.SagaEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 推进单个事务的执行引擎。
 *
 * <p>锁约定：{@code drive} 接收协调器给出的“每事务锁”，锁只保护
 * <b>状态迁移 + 事件落盘 + 聚合更新</b> 这段临界区；对参与者的实际调用
 * （execute/compensate，可能长时间阻塞或超时）在锁外执行。这样：
 * <ul>
 *   <li>状态机永远只在持锁时被读写，并发提交看不到撕裂的中间迁移；</li>
 *   <li>某驱动线程被挂死的参与者卡住时，不持有事务锁，超时巡检器可以
 *       接管并把事务收敛；</li>
 *   <li>锁外等待结束后重新拿锁，若事务已被巡检器收敛（状态不再是
 *       RUNNING），本次驱动立即放弃，绝不覆盖新状态。</li>
 * </ul>
 *
 * <p>补偿规则：任一步骤失败/超时后，对结果未知步骤与所有已完成步骤按
 * 逆序补偿；补偿有独立超时与重试；仍失败则 COMPENSATION_FAILED，绝不
 * 静默丢弃。
 */
public class SagaEngine {

    private static final Logger log = LoggerFactory.getLogger(SagaEngine.class);

    private final StepRegistry registry;
    private final EventLog eventLog;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ExecutorService stepExecutors;

    public SagaEngine(StepRegistry registry, EventLog eventLog) {
        this.registry = registry;
        this.eventLog = eventLog;
        AtomicInteger counter = new AtomicInteger();
        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "saga-step-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
        this.stepExecutors = Executors.newCachedThreadPool(tf);
    }

    /** 创建新事务并把 SAGA_CREATED 落盘（调用方已持有事务锁）。 */
    public SagaInstance create(String sagaId, SagaDefinition definition) {
        definition.steps().forEach(registry::register);
        SagaContext context = new SagaContext(sagaId);
        SagaInstance instance = new SagaInstance(sagaId, definition, context);

        DefinitionEnvelope envelope = new DefinitionEnvelope(
                definition.type(),
                definition.steps().stream().map(SagaStep::name).toList(),
                definition.stepTimeout().toMillis(),
                definition.sagaTimeout().toMillis(),
                definition.compensationRetries());
        String detail;
        try {
            detail = mapper.writeValueAsString(envelope);
        } catch (Exception e) {
            throw new SagaException("定义序列化失败: " + sagaId, e);
        }
        eventLog.append(SagaEvent.of(sagaId, EventType.SAGA_CREATED, null,
                detail, FailureReason.NONE));
        transitLocked(instance, SagaState.RUNNING);
        log.info("[{}] 状态迁移轨迹: STARTED -> RUNNING (type={}, steps={})",
                sagaId, definition.type(), envelope.stepNames());
        return instance;
    }

    /** 驱动事务。{@code lock} 为该事务独占锁，参与者调用在锁外进行。 */
    public void drive(SagaInstance instance, Object lock) {
        if (instance.isTerminal()) {
            return;
        }
        SagaState state;
        synchronized (lock) {
            if (instance.isTerminal()) {
                return;
            }
            state = instance.getState();
        }
        if (state == SagaState.COMPENSATING) {
            runCompensation(instance, lock);
            return;
        }
        runForward(instance, lock);
    }

    private void runForward(SagaInstance instance, Object lock) {
        SagaDefinition def = instance.getDefinition();
        while (true) {
            int index;
            SagaStep step;
            StepStateRecord record;
            boolean recoveredRunning;
            synchronized (lock) {
                if (instance.isTerminal()) {
                    return;
                }
                if (instance.getState() != SagaState.RUNNING) {
                    // 等待期间已被巡检器/其它驱动收敛
                    return;
                }
                if (isSagaTimedOut(instance)) {
                    int anchor = Math.min(instance.getCurrentStepIndex(),
                            def.steps().size() - 1);
                    beginCompensationLocked(instance, anchor,
                            EventType.STEP_TIMED_OUT, FailureReason.SAGA_TIMEOUT,
                            "事务总超时到达，中止正向推进");
                    runCompensation(instance, lock);
                    return;
                }
                index = instance.getCurrentStepIndex();
                if (index >= def.steps().size()) {
                    finishLocked(instance, SagaState.COMPLETED, FailureReason.NONE,
                            "全部步骤提交完成");
                    return;
                }
                step = def.steps().get(index);
                record = instance.getStepRecords().get(index);
                if (record.getState() == StepState.COMPLETED) {
                    instance.setCurrentStepIndex(index + 1);
                    continue;
                }

                recoveredRunning = record.getState() == StepState.RUNNING;
                record.incAttempts();
                emitLocked(instance, EventType.STEP_STARTED, index,
                        (recoveredRunning ? "恢复后续跑检查: " : "开始执行步骤 ")
                                + step.name(), FailureReason.NONE);
                record.setState(StepState.RUNNING);
                log.info("[{}] 步骤轨迹: #{} [{}] 第{}次提交开始",
                        instance.getSagaId(), index, step.name(), record.getAttempts());
            }

            // 结果未知的“恢复后 RUNNING”步骤不允许乐观重跑：直接按超时收敛
            if (recoveredRunning) {
                synchronized (lock) {
                    if (instance.isTerminal()
                            || instance.getState() != SagaState.RUNNING) {
                        return;
                    }
                    beginCompensationLocked(instance, index,
                            EventType.STEP_TIMED_OUT, FailureReason.STEP_TIMEOUT,
                            "重启恢复时步骤 " + step.name() + " 结果未知，按超时补偿");
                    log.warn("[{}] 恢复轨迹: #{} [{}] 崩溃前结果未知 -> STEP_TIMED_OUT -> 逆序补偿",
                            instance.getSagaId(), index, step.name());
                }
                runCompensation(instance, lock);
                return;
            }

            // 锁外调用参与者：驱动线程即使被卡住也不阻塞巡检器接管
            StepOutcome outcome = invokeWithTimeout(step, instance.getContext(),
                    true, def.stepTimeout().toMillis());

            synchronized (lock) {
                if (instance.isTerminal()
                        || instance.getState() != SagaState.RUNNING) {
                    log.info("[{}] 步骤 [{}] 锁外返回时事务已被其它路径收敛({})，放弃本次驱动",
                            instance.getSagaId(), step.name(), instance.getState());
                    return;
                }
                if (outcome == null) {
                    beginCompensationLocked(instance, index,
                            EventType.STEP_TIMED_OUT, FailureReason.STEP_TIMEOUT,
                            "步骤 " + step.name() + " 执行超过 " + def.stepTimeout());
                    log.warn("[{}] 步骤轨迹: #{} [{}] 提交超时 -> 进入逆序补偿",
                            instance.getSagaId(), index, step.name());
                } else if (!outcome.success()) {
                    beginCompensationLocked(instance, index,
                            EventType.STEP_FAILED, FailureReason.STEP_FAILED,
                            "步骤 " + step.name() + " 失败: " + outcome.message());
                    log.warn("[{}] 步骤轨迹: #{} [{}] 提交失败({}) -> 进入逆序补偿",
                            instance.getSagaId(), index, step.name(), outcome.message());
                } else {
                    record.setLastMessage(outcome.message());
                    emitLocked(instance, EventType.STEP_COMPLETED, index,
                            outcome.message(), FailureReason.NONE);
                    record.setState(StepState.COMPLETED);
                    instance.setCurrentStepIndex(index + 1);
                    log.info("[{}] 步骤轨迹: #{} [{}] 提交完成",
                            instance.getSagaId(), index, step.name());
                    continue;
                }
            }
            runCompensation(instance, lock);
            return;
        }
    }

    private void runCompensation(SagaInstance instance, Object lock) {
        SagaDefinition def = instance.getDefinition();
        int anchor;
        synchronized (lock) {
            anchor = instance.getCompensationFromIndex();
            if (anchor < 0) {
                anchor = instance.getCurrentStepIndex();
            }
            anchor = Math.min(anchor, def.steps().size() - 1);
            instance.setCompensationFromIndex(anchor);
        }

        for (int index = anchor; index >= 0; index--) {
            SagaStep step = def.steps().get(index);
            StepStateRecord record;
            boolean needCompensate;
            synchronized (lock) {
                if (instance.isTerminal()) {
                    return;
                }
                if (instance.getState() != SagaState.COMPENSATING) {
                    return;
                }
                record = instance.getStepRecords().get(index);
                needCompensate = record.getState() != StepState.COMPENSATED
                        && record.getState() != StepState.PENDING;
            }
            if (!needCompensate) {
                continue;
            }

            int maxAttempts = 1 + Math.max(0, def.compensationRetries());
            boolean compensated = false;
            String lastDetail = "";
            for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                synchronized (lock) {
                    if (instance.isTerminal()) {
                        return;
                    }
                    record.incCompensationAttempts();
                    emitLocked(instance, EventType.STEP_COMPENSATION_STARTED, index,
                            "第" + attempt + "次补偿步骤 " + step.name(),
                            FailureReason.NONE);
                    record.setState(StepState.COMPENSATING);
                    log.info("[{}] 步骤轨迹: #{} [{}] 第{}次补偿开始",
                            instance.getSagaId(), index, step.name(), attempt);
                }

                StepOutcome outcome = invokeWithTimeout(step, instance.getContext(),
                        false, def.stepTimeout().toMillis());

                synchronized (lock) {
                    if (instance.isTerminal()) {
                        return;
                    }
                    if (outcome != null && outcome.success()) {
                        compensated = true;
                        break;
                    }
                    lastDetail = outcome == null
                            ? "补偿超时，超过 " + def.stepTimeout()
                            : "补偿返回失败: " + outcome.message();
                    log.warn("[{}] 步骤轨迹: #{} [{}] 第{}次补偿未成功: {}",
                            instance.getSagaId(), index, step.name(), attempt, lastDetail);
                }
            }

            synchronized (lock) {
                if (instance.isTerminal()) {
                    return;
                }
                if (compensated) {
                    emitLocked(instance, EventType.STEP_COMPENSATED, index,
                            "步骤 " + step.name() + " 补偿完成", FailureReason.NONE);
                    record.setState(StepState.COMPENSATED);
                    log.info("[{}] 步骤轨迹: #{} [{}] 补偿完成",
                            instance.getSagaId(), index, step.name());
                } else {
                    emitLocked(instance, EventType.STEP_COMPENSATION_FAILED, index,
                            "步骤 " + step.name() + " " + lastDetail
                                    + "，已重试 " + def.compensationRetries() + " 次",
                            FailureReason.COMPENSATION_FAILED);
                    record.setState(StepState.COMPENSATION_FAILED);
                    finishLocked(instance, SagaState.COMPENSATION_FAILED,
                            FailureReason.COMPENSATION_FAILED,
                            "步骤 " + step.name() + " 补偿失败: " + lastDetail);
                    return;
                }
            }
        }
        synchronized (lock) {
            if (!instance.isTerminal()) {
                finishLocked(instance, SagaState.COMPENSATED, FailureReason.NONE,
                        "逆序补偿全部完成");
            }
        }
    }

    /**
     * 超时巡检器对“长时间无进展”事务的接管入口。调用方在事务锁内调用。
     * 把当前步骤标记超时并切换到补偿态，随后在锁外执行逆序补偿。
     */
    public void convergeStuckStep(SagaInstance instance, Object lock,
                                  FailureReason reason, String detail) {
        synchronized (lock) {
            if (instance.isTerminal() || instance.getState() != SagaState.RUNNING) {
                return;
            }
            int index = Math.min(instance.getCurrentStepIndex(),
                    instance.getDefinition().steps().size() - 1);
            SagaStep step = instance.getDefinition().steps().get(index);
            beginCompensationLocked(instance, index, EventType.STEP_TIMED_OUT, reason, detail);
            log.warn("[{}] 巡检轨迹: #{} [{}] 被判定超时({}) -> 进入逆序补偿",
                    instance.getSagaId(), index, step.name(), reason);
        }
        runCompensation(instance, lock);
    }

    private void beginCompensationLocked(SagaInstance instance, int failedIndex,
                                         EventType triggerEvent, FailureReason reason,
                                         String detail) {
        SagaState from = instance.getState();
        SagaStateMachine.requireTransit(from, SagaState.COMPENSATING);
        emitLocked(instance, triggerEvent, failedIndex, detail, reason);
        instance.getStepRecords().get(failedIndex).setState(StepState.FAILED);
        emitLocked(instance, EventType.SAGA_COMPENSATION_STARTED, failedIndex,
                "从事务状态 " + from + " 进入逆序补偿", FailureReason.NONE);
        instance.setState(SagaState.COMPENSATING);
        instance.setCurrentStepIndex(failedIndex);
        instance.setCompensationFromIndex(failedIndex);
        log.info("[{}] 状态迁移轨迹: {} -> COMPENSATING ({})",
                instance.getSagaId(), from, reason);
    }

    private boolean isSagaTimedOut(SagaInstance instance) {
        long elapsed = System.currentTimeMillis() - instance.getCreatedAt();
        return elapsed > instance.getDefinition().sagaTimeout().toMillis();
    }

    /**
     * 锁外调用参与者：成功/业务失败返回 {@link StepOutcome}，超时返回 null。
     * 参与者抛出的异常视为业务失败。
     */
    private StepOutcome invokeWithTimeout(SagaStep step, SagaContext context,
                                          boolean forward, long timeoutMillis) {
        Future<StepOutcome> future = stepExecutors.submit(() ->
                forward ? step.execute(context) : step.compensate(context));
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            return null;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            log.warn("[{}] 参与者 [{}] 抛出异常: {}",
                    context.getSagaId(), step.name(), cause.toString());
            return StepOutcome.fail(cause.toString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return StepOutcome.fail("协调器被中断");
        }
    }

    private void emitLocked(SagaInstance instance, EventType type, Integer stepIndex,
                            String detail, FailureReason reason) {
        eventLog.append(SagaEvent.of(instance.getSagaId(), type, stepIndex, detail, reason));
    }

    private void transitLocked(SagaInstance instance, SagaState target) {
        SagaState from = instance.getState();
        SagaStateMachine.requireTransit(from, target);
        instance.setState(target);
        log.info("[{}] 状态迁移轨迹: {} -> {}", instance.getSagaId(), from, target);
    }

    private void finishLocked(SagaInstance instance, SagaState target,
                              FailureReason reason, String detail) {
        SagaState from = instance.getState();
        SagaStateMachine.requireTransit(from, target);
        instance.setReason(reason);
        instance.setReasonDetail(detail);
        emitLocked(instance, EventType.SAGA_FINISHED, null, target.name(), reason);
        instance.setState(target);
        log.info("[{}] 状态迁移轨迹: {} -> {} (reason={}, detail={})",
                instance.getSagaId(), from, target, reason, detail);
    }

    public void shutdown() {
        stepExecutors.shutdownNow();
    }
}
