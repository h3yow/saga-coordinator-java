package com.github.h3yow.sagacoordinatorjava.coordinator;

import com.github.h3yow.sagacoordinatorjava.model.EventType;
import com.github.h3yow.sagacoordinatorjava.model.FailureReason;
import com.github.h3yow.sagacoordinatorjava.model.SagaDefinition;
import com.github.h3yow.sagacoordinatorjava.model.SagaStepDefinition;
import com.github.h3yow.sagacoordinatorjava.model.SagaTransaction;
import com.github.h3yow.sagacoordinatorjava.model.StateMachine;
import com.github.h3yow.sagacoordinatorjava.model.StepRecord;
import com.github.h3yow.sagacoordinatorjava.model.StepStatus;
import com.github.h3yow.sagacoordinatorjava.model.TransactionEvent;
import com.github.h3yow.sagacoordinatorjava.model.TransactionResult;
import com.github.h3yow.sagacoordinatorjava.model.TransactionStatus;
import com.github.h3yow.sagacoordinatorjava.saga.SagaRegistry;
import com.github.h3yow.sagacoordinatorjava.store.TransactionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Saga 协调器：提交事务、驱动正向步骤、失败后逆序补偿、返回可解释结论。
 *
 * <p>同一事务 ID 的重复 / 并发提交只会有一次有效业务效果，
 * 后续调用返回同一个 {@link TransactionResult}。
 * 所有状态迁移先校验合法性，落盘后才继续推进，崩溃重启可据此恢复。
 */
public class SagaCoordinator {

    private static final Logger log = LoggerFactory.getLogger(SagaCoordinator.class);

    private final TransactionStore store;
    private final SagaRegistry registry;
    private final TransactionLockManager lockManager;
    private final long stepTimeoutMillis;
    private final long transactionTimeoutMillis;
    private final int compensationMaxRetries;
    private final ExecutorService stepExecutor;

    public SagaCoordinator(TransactionStore store, SagaRegistry registry,
                           TransactionLockManager lockManager,
                           long stepTimeoutMillis, long transactionTimeoutMillis,
                           int compensationMaxRetries) {
        this.store = store;
        this.registry = registry;
        this.lockManager = lockManager;
        this.stepTimeoutMillis = stepTimeoutMillis;
        this.transactionTimeoutMillis = transactionTimeoutMillis;
        this.compensationMaxRetries = compensationMaxRetries;
        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "saga-step-executor");
            t.setDaemon(true);
            return t;
        };
        this.stepExecutor = Executors.newCachedThreadPool(tf);
    }

    /**
     * 显式注册 Saga 定义。应用通常在启动时完成注册，以便崩溃恢复时使用。
     * {@link #submit} 也会顺带注册，便于单进程直接使用。
     */
    public <T> void register(SagaDefinition<T> definition, java.util.function.Supplier<T> contextFactory) {
        registry.register(definition, contextFactory);
    }

    /**
     * 提交（或取回）一个事务。重复 / 并发提交返回同一结论。
     *
     * @param definition    Saga 定义
     * @param transactionId 调用方指定的幂等键
     * @param context       步骤共享上下文
     */
    public <T> TransactionResult submit(SagaDefinition<T> definition, String transactionId, T context) {
        registry.register(definition, () -> context);
        ReentrantLock lock = lockManager.get(transactionId);
        lock.lock();
        try {
            Optional<SagaTransaction> existing = store.find(transactionId);
            SagaTransaction tx;
            if (existing.isPresent()) {
                tx = existing.get();
                log.info("tx={} duplicate submit, currentStatus={} (no new business effect)",
                        transactionId, tx.getStatus());
            } else {
                tx = createTransaction(definition, transactionId);
                store.createIfAbsent(tx);
                log.info("tx={} created, sagaType={}, steps={}",
                        transactionId, definition.getType(), definition.getSteps().size());
            }
            return drive(tx);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 从磁盘加载事务并按既定规则继续推进（崩溃恢复入口）。
     */
    public TransactionResult recover(String transactionId) {
        ReentrantLock lock = lockManager.get(transactionId);
        lock.lock();
        try {
            SagaTransaction tx = store.find(transactionId)
                    .orElseThrow(() -> new IllegalArgumentException("transaction not found: " + transactionId));
            addEvent(tx, EventType.RECOVERY_RESUMED, null,
                    "resume from status " + tx.getStatus());
            store.save(tx);
            log.info("tx={} recovery resumed from status={}", transactionId, tx.getStatus());
            return drive(tx);
        } finally {
            lock.unlock();
        }
    }

    public Optional<TransactionResult> getResult(String transactionId) {
        return store.find(transactionId).map(this::toResult);
    }

    // ------------------------------------------------------------------
    // 正向推进
    // ------------------------------------------------------------------

    private <T> SagaTransaction createTransaction(SagaDefinition<T> definition, String transactionId) {
        long now = System.currentTimeMillis();
        SagaTransaction tx = new SagaTransaction();
        tx.setTransactionId(transactionId);
        tx.setSagaType(definition.getType());
        tx.setStatus(TransactionStatus.PENDING);
        tx.setFailureReason(FailureReason.NONE);
        tx.setCurrentStepIndex(0);
        tx.setCreatedAt(now);
        tx.setUpdatedAt(now);
        tx.setDeadlineAt(now + transactionTimeoutMillis);
        for (SagaStepDefinition<T> step : definition.getSteps()) {
            StepRecord record = new StepRecord();
            record.setName(step.getName());
            record.setStatus(StepStatus.INIT);
            record.setAttempts(0);
            record.setUpdatedAt(now);
            tx.getSteps().add(record);
        }
        addEvent(tx, EventType.CREATED, null,
                definition.getSteps().size() + " steps, timeout=" + transactionTimeoutMillis + "ms");
        transitionTx(tx, TransactionStatus.RUNNING);
        return tx;
    }

    /**
     * 根据持久化状态继续把事务推进到终态。非重入安全：调用方必须持有该事务锁。
     */
    private TransactionResult drive(SagaTransaction tx) {
        SagaDefinition<Object> definition = resolveDefinition(tx);
        switch (tx.getStatus()) {
            case PENDING, RUNNING -> runForward(tx, definition);
            case COMPENSATING, TIMEOUT_COMPENSATING -> runCompensation(tx, definition);
            default -> {
                // 终态：直接返回既有结论
            }
        }
        return toResult(tx);
    }

    private void runForward(SagaTransaction tx, SagaDefinition<Object> definition) {
        if (tx.getStatus() == TransactionStatus.PENDING) {
            transitionTx(tx, TransactionStatus.RUNNING);
            store.save(tx);
        }
        while (tx.getCurrentStepIndex() < definition.getSteps().size()) {
            if (isTransactionTimedOut(tx, System.currentTimeMillis())) {
                markTimeoutAndCompensate(tx, definition,
                        "transaction deadline " + tx.getDeadlineAt() + " exceeded");
                return;
            }
            int index = tx.getCurrentStepIndex();
            SagaStepDefinition<Object> step = definition.getSteps().get(index);
            StepRecord record = tx.getSteps().get(index);

            // 崩溃可能发生在"已落盘 EXECUTING、业务结果未知"之后：幂等重试该步骤
            if (record.getStatus() != StepStatus.EXECUTING) {
                transitionStep(record, StepStatus.EXECUTING);
                addEvent(tx, EventType.STEP_STARTED, step.getName(),
                        "attempt " + (record.getAttempts() + 1));
                store.save(tx);
            }
            record.setAttempts(record.getAttempts() + 1);

            StepOutcome outcome = executeWithTimeout(step, tx, index);
            record.setUpdatedAt(System.currentTimeMillis());
            if (outcome.timedOut()) {
                record.setStatus(StepStatus.TIMED_OUT);
                record.setLastError("step timeout after " + stepTimeoutMillis + "ms");
                addEvent(tx, EventType.STEP_TIMED_OUT, step.getName(), record.getLastError());
                startCompensation(tx, FailureReason.STEP_TIMEOUT,
                        "step '" + step.getName() + "' timed out");
                store.save(tx);
                runCompensation(tx, definition);
                return;
            } else if (outcome.error() != null) {
                record.setStatus(StepStatus.FAILED);
                record.setLastError(outcome.error());
                addEvent(tx, EventType.STEP_FAILED, step.getName(), outcome.error());
                startCompensation(tx, FailureReason.STEP_FAILED,
                        "step '" + step.getName() + "' failed: " + outcome.error());
                store.save(tx);
                runCompensation(tx, definition);
                return;
            } else {
                transitionStep(record, StepStatus.COMPLETED);
                addEvent(tx, EventType.STEP_COMPLETED, step.getName(), null);
                tx.setCurrentStepIndex(index + 1);
                touch(tx);
                store.save(tx);
            }
        }
        transitionTx(tx, TransactionStatus.COMPLETED);
        store.save(tx);
        log.info("tx={} COMPLETED", tx.getTransactionId());
    }

    // ------------------------------------------------------------------
    // 逆序补偿
    // ------------------------------------------------------------------

    private void startCompensation(SagaTransaction tx, FailureReason reason, String detail) {
        tx.setFailureReason(reason);
        tx.setFailureDetail(detail);
        TransactionStatus target = reason == FailureReason.TRANSACTION_TIMEOUT
                ? TransactionStatus.TIMEOUT_COMPENSATING
                : TransactionStatus.COMPENSATING;
        transitionTx(tx, target);
        addEvent(tx, EventType.COMPENSATION_STARTED, null, detail);
    }

    private void runCompensation(SagaTransaction tx, SagaDefinition<Object> definition) {
        boolean timeoutDriven = tx.getStatus() == TransactionStatus.TIMEOUT_COMPENSATING;
        // 逆序：只补偿确实 COMPLETED 的步骤；FAILED/TIMED_OUT 步骤没有产生业务效果，跳过
        for (int index = tx.getCurrentStepIndex() - 1; index >= 0; index--) {
            SagaStepDefinition<Object> step = definition.getSteps().get(index);
            StepRecord record = tx.getSteps().get(index);
            if (record.getStatus() == StepStatus.COMPENSATED
                    || record.getStatus() == StepStatus.FAILED
                    || record.getStatus() == StepStatus.TIMED_OUT
                    || record.getStatus() == StepStatus.INIT) {
                continue;
            }
            if (record.getStatus() != StepStatus.COMPENSATING) {
                transitionStep(record, StepStatus.COMPENSATING);
                store.save(tx);
            }
            boolean compensated = false;
            String lastError = null;
            for (int attempt = 1; attempt <= compensationMaxRetries; attempt++) {
                tx.setCompensationRetries(tx.getCompensationRetries() + 1);
                StepOutcome outcome = executeCompensationWithTimeout(step, tx, index);
                if (!outcome.timedOut() && outcome.error() == null) {
                    compensated = true;
                    break;
                }
                lastError = outcome.timedOut()
                        ? "compensation timeout after " + stepTimeoutMillis + "ms"
                        : outcome.error();
                log.warn("tx={} compensation of step '{}' attempt {}/{} failed: {}",
                        tx.getTransactionId(), step.getName(), attempt,
                        compensationMaxRetries, lastError);
                store.save(tx);
            }
            if (compensated) {
                transitionStep(record, StepStatus.COMPENSATED);
                addEvent(tx, EventType.STEP_COMPENSATED, step.getName(), null);
                store.save(tx);
            } else {
                // 补偿无法收敛：明确标记，绝不静默丢弃，转人工处理
                addEvent(tx, EventType.STEP_COMPENSATION_FAILED, step.getName(), lastError);
                transitionTx(tx, TransactionStatus.COMPENSATION_FAILED);
                tx.setFailureReason(FailureReason.COMPENSATION_FAILED);
                tx.setFailureDetail("compensation of step '" + step.getName()
                        + "' did not converge after " + compensationMaxRetries
                        + " retries: " + lastError);
                record.setLastError(lastError);
                store.save(tx);
                log.error("tx={} COMPENSATION_FAILED at step '{}': {}",
                        tx.getTransactionId(), step.getName(), lastError);
                return;
            }
        }
        transitionTx(tx, timeoutDriven
                ? TransactionStatus.TIMED_OUT_COMPENSATED
                : TransactionStatus.COMPENSATED);
        store.save(tx);
        log.info("tx={} {}", tx.getTransactionId(), tx.getStatus());
    }

    /**
     * 事务级超时处理入口（巡检调用）：若仍有在途步骤则先打超时标记，再走补偿。
     */
    public void timeoutTransaction(String transactionId) {
        ReentrantLock lock = lockManager.get(transactionId);
        lock.lock();
        try {
            SagaTransaction tx = store.find(transactionId)
                    .orElseThrow(() -> new IllegalArgumentException("transaction not found: " + transactionId));
            if (StateMachine.isTerminal(tx.getStatus())) {
                return;
            }
            SagaDefinition<Object> definition = resolveDefinition(tx);
            addEvent(tx, EventType.RECOVERY_TIMEOUT, null,
                    "swept at " + System.currentTimeMillis());
            markTimeoutAndCompensate(tx, definition,
                    "no progress before transaction timeout " + tx.getDeadlineAt());
        } finally {
            lock.unlock();
        }
    }

    private void markTimeoutAndCompensate(SagaTransaction tx, SagaDefinition<Object> definition,
                                          String detail) {
        if (StateMachine.isTerminal(tx.getStatus())) {
            return;
        }
        // 仅把真正在途（EXECUTING、结果未知）的当前步骤标记为 TIMED_OUT：
        // 它没有可确认的业务效果，因此不纳入补偿；INIT 步骤从未执行，保持不变
        if (tx.getCurrentStepIndex() < tx.getSteps().size()) {
            StepRecord inFlight = tx.getSteps().get(tx.getCurrentStepIndex());
            if (inFlight.getStatus() == StepStatus.EXECUTING) {
                transitionStep(inFlight, StepStatus.TIMED_OUT);
            }
        }
        tx.setFailureReason(FailureReason.TRANSACTION_TIMEOUT);
        tx.setFailureDetail(detail);
        transitionTx(tx, TransactionStatus.TIMEOUT_COMPENSATING);
        addEvent(tx, EventType.COMPENSATION_STARTED, null, detail);
        store.save(tx);
        runCompensation(tx, definition);
    }

    // ------------------------------------------------------------------
    // 步骤执行（带超时）与恢复
    // ------------------------------------------------------------------

    private StepOutcome executeWithTimeout(SagaStepDefinition<Object> step,
                                           SagaTransaction tx, int index) {
        Object context = resolveContext(tx);
        Future<?> future = stepExecutor.submit(() -> {
            step.getAction().execute(context);
            return null;
        });
        try {
            if (stepTimeoutMillis <= 0) {
                future.get();
            } else {
                future.get(stepTimeoutMillis, TimeUnit.MILLISECONDS);
            }
            return StepOutcome.success();
        } catch (TimeoutException e) {
            future.cancel(true);
            return StepOutcome.timeout();
        } catch (ExecutionException e) {
            return StepOutcome.error(rootMessage(e));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return StepOutcome.error("interrupted");
        }
    }

    private StepOutcome executeCompensationWithTimeout(SagaStepDefinition<Object> step,
                                                       SagaTransaction tx, int index) {
        Object context = resolveContext(tx);
        Future<?> future = stepExecutor.submit(() -> {
            step.getCompensation().execute(context);
            return null;
        });
        try {
            if (stepTimeoutMillis <= 0) {
                future.get();
            } else {
                future.get(stepTimeoutMillis, TimeUnit.MILLISECONDS);
            }
            return StepOutcome.success();
        } catch (TimeoutException e) {
            future.cancel(true);
            return StepOutcome.timeout();
        } catch (ExecutionException e) {
            return StepOutcome.error(rootMessage(e));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return StepOutcome.error("interrupted");
        }
    }

    private boolean isTransactionTimedOut(SagaTransaction tx, long now) {
        return tx.getDeadlineAt() > 0 && now > tx.getDeadlineAt();
    }

    @SuppressWarnings("unchecked")
    private SagaDefinition<Object> resolveDefinition(SagaTransaction tx) {
        return (SagaDefinition<Object>) registry.get(tx.getSagaType())
                .map(r -> r.definition())
                .orElseThrow(() -> new IllegalStateException(
                        "saga definition not registered for type: " + tx.getSagaType()
                                + " (register it before recovery)"));
    }

    private Object resolveContext(SagaTransaction tx) {
        return registry.get(tx.getSagaType())
                .map(r -> r.contextFactory().get())
                .orElseThrow(() -> new IllegalStateException(
                        "context factory missing for type: " + tx.getSagaType()));
    }

    // ------------------------------------------------------------------
    // 辅助
    // ------------------------------------------------------------------

    private void transitionTx(SagaTransaction tx, TransactionStatus to) {
        StateMachine.assertTxTransition(tx.getStatus(), to);
        TransactionStatus from = tx.getStatus();
        tx.setStatus(to);
        touch(tx);
        addEvent(tx, EventType.STATE_CHANGED, null, from + " -> " + to);
        log.info("tx={} state {} -> {}", tx.getTransactionId(), from, to);
    }

    private void transitionStep(StepRecord record, StepStatus to) {
        StateMachine.assertStepTransition(record.getStatus(), to);
        record.setStatus(to);
        record.setUpdatedAt(System.currentTimeMillis());
    }

    private void touch(SagaTransaction tx) {
        tx.setUpdatedAt(System.currentTimeMillis());
    }

    private void addEvent(SagaTransaction tx, EventType type, String step, String detail) {
        tx.getEvents().add(new TransactionEvent(System.currentTimeMillis(), type, step, detail));
    }

    private TransactionResult toResult(SagaTransaction tx) {
        return new TransactionResult(
                tx.getTransactionId(),
                tx.getStatus(),
                tx.getFailureReason(),
                tx.getFailureDetail(),
                List.copyOf(tx.getEvents()));
    }

    private static String rootMessage(Throwable throwable) {
        Throwable t = throwable;
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return t.getClass().getSimpleName() + ": " + t.getMessage();
    }

    private record StepOutcome(boolean timedOut, String error) {
        static StepOutcome success() {
            return new StepOutcome(false, null);
        }

        static StepOutcome timeout() {
            return new StepOutcome(true, null);
        }

        static StepOutcome error(String error) {
            return new StepOutcome(false, error);
        }
    }
}
