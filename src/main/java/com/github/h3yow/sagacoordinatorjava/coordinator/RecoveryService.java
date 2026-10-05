package com.github.h3yow.sagacoordinatorjava.coordinator;

import com.github.h3yow.sagacoordinatorjava.model.SagaTransaction;
import com.github.h3yow.sagacoordinatorjava.model.StateMachine;
import com.github.h3yow.sagacoordinatorjava.store.TransactionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * 启动恢复 + 定时巡检：
 * 重启后继续未完成事务；长时间无进展的事务按超时规则收敛。
 *
 * <p>隔离保证：
 * <ul>
 *     <li>逐事务独立加锁；超时巡检拿不到分段锁（该事务正被其它线程驱动）就跳过本轮，
 *         绝不在锁上无限等待，一个卡住的事务不会拖住对其它事务的处置；</li>
 *     <li>恢复 / 巡检任务全部提交到独立小线程池并行执行，再统一限时等待，
 *         单任务超时或异常被隔离，只影响该事务本轮，不阻塞其它事务；</li>
 *     <li>COMPENSATION_FAILED / MANUALLY_RESOLVED 等终态永不自动重放。</li>
 * </ul>
 */
public class RecoveryService {

    private static final Logger log = LoggerFactory.getLogger(RecoveryService.class);

    /** 单个事务恢复 / 收敛任务的执行上限：到点取消，不拖垮整轮巡检。 */
    private static final long TASK_TIMEOUT_MILLIS = 30_000;

    private final TransactionStore store;
    private final SagaCoordinator coordinator;
    private final ExecutorService recoveryExecutor;

    public RecoveryService(TransactionStore store, SagaCoordinator coordinator) {
        this.store = store;
        this.coordinator = coordinator;
        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "saga-recovery");
            t.setDaemon(true);
            return t;
        };
        this.recoveryExecutor = Executors.newFixedThreadPool(4, tf);
    }

    /**
     * 恢复所有非终态事务（进程启动时调用）。多事务并行，逐事务异常隔离。
     */
    public void recoverAll() {
        List<SagaTransaction> all = store.findAll();
        log.info("recovery scan: {} persisted transactions", all.size());
        List<Pending> tasks = new ArrayList<>();
        for (SagaTransaction tx : all) {
            if (StateMachine.isTerminal(tx.getStatus())) {
                continue;
            }
            tasks.add(submitIsolated("recover", tx.getTransactionId(),
                    () -> coordinator.recover(tx.getTransactionId())));
        }
        awaitAll(tasks);
    }

    /**
     * 扫描超过事务级超时时间仍未推进的事务并收敛（定时调用）。
     * 所有超时事务并行处置，单个卡住 / 异常不影响其它事务与后续巡检轮次。
     */
    public void sweepTimeouts(long now) {
        List<Pending> tasks = new ArrayList<>();
        for (SagaTransaction tx : store.findAll()) {
            if (StateMachine.isTerminal(tx.getStatus())) {
                continue;
            }
            if (tx.getDeadlineAt() > 0 && now > tx.getDeadlineAt()) {
                String txId = tx.getTransactionId();
                log.warn("tx={} timed out (deadline={}), scheduling sweep",
                        txId, tx.getDeadlineAt());
                tasks.add(submitIsolated("sweep", txId, () -> {
                    boolean handled = coordinator.tryTimeoutTransaction(txId);
                    if (!handled) {
                        log.debug("tx={} sweep skipped (busy), next round will retry", txId);
                    }
                }));
            }
        }
        awaitAll(tasks);
    }

    private Pending submitIsolated(String action, String txId, Runnable task) {
        Future<?> future = recoveryExecutor.submit(() -> {
            try {
                task.run();
            } catch (Exception e) {
                log.error("tx={} {} failed: {}", txId, action, e.getMessage(), e);
            }
        });
        return new Pending(action, txId, future);
    }

    private void awaitAll(List<Pending> tasks) {
        for (Pending p : tasks) {
            try {
                p.future().get(TASK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            } catch (java.util.concurrent.TimeoutException e) {
                p.future().cancel(true);
                log.error("tx={} {} timed out after {}ms, skipped this round (other tx unaffected)",
                        p.txId(), p.action(), TASK_TIMEOUT_MILLIS);
            } catch (Exception e) {
                log.error("tx={} {} scheduling failed: {}",
                        p.txId(), p.action(), e.getMessage(), e);
            }
        }
    }

    private record Pending(String action, String txId, Future<?> future) {
    }
}
