package com.github.h3yow.sagacoordinatorjava.coordinator;

import com.github.h3yow.sagacoordinatorjava.model.SagaTransaction;
import com.github.h3yow.sagacoordinatorjava.model.StateMachine;
import com.github.h3yow.sagacoordinatorjava.store.TransactionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 启动恢复 + 定时巡检：
 * 重启后继续未完成事务；长时间无进展的事务按超时规则收敛，
 * 逐事务独立处理，单个事务卡住不阻塞其它事务。
 */
public class RecoveryService {

    private static final Logger log = LoggerFactory.getLogger(RecoveryService.class);

    private final TransactionStore store;
    private final SagaCoordinator coordinator;

    public RecoveryService(TransactionStore store, SagaCoordinator coordinator) {
        this.store = store;
        this.coordinator = coordinator;
    }

    /**
     * 恢复所有非终态事务（进程启动时调用）。逐事务独立加锁、独立异常处理。
     */
    public void recoverAll() {
        List<SagaTransaction> all = store.findAll();
        log.info("recovery scan: {} persisted transactions", all.size());
        for (SagaTransaction tx : all) {
            if (StateMachine.isTerminal(tx.getStatus())) {
                continue;
            }
            try {
                coordinator.recover(tx.getTransactionId());
            } catch (Exception e) {
                // 一个事务恢复失败不影响其它事务
                log.error("tx={} recovery failed: {}", tx.getTransactionId(), e.getMessage(), e);
            }
        }
    }

    /**
     * 扫描超过事务级超时时间仍未推进的事务并收敛（定时调用）。
     * 每个事务独立处理，异常互相隔离。
     */
    public void sweepTimeouts(long now) {
        for (SagaTransaction tx : store.findAll()) {
            if (StateMachine.isTerminal(tx.getStatus())) {
                continue;
            }
            if (tx.getDeadlineAt() > 0 && now > tx.getDeadlineAt()) {
                try {
                    log.warn("tx={} timed out (deadline={}), sweeping",
                            tx.getTransactionId(), tx.getDeadlineAt());
                    coordinator.timeoutTransaction(tx.getTransactionId());
                } catch (Exception e) {
                    log.error("tx={} timeout sweep failed: {}",
                            tx.getTransactionId(), e.getMessage(), e);
                }
            }
        }
    }
}
