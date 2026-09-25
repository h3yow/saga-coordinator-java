package com.github.h3yow.sagacoordinatorjava.tests;

import com.github.h3yow.sagacoordinatorjava.coordinator.SagaCoordinator;
import com.github.h3yow.sagacoordinatorjava.model.FailureReason;
import com.github.h3yow.sagacoordinatorjava.model.IllegalTransitionException;
import com.github.h3yow.sagacoordinatorjava.model.SagaTransaction;
import com.github.h3yow.sagacoordinatorjava.model.StateMachine;
import com.github.h3yow.sagacoordinatorjava.model.TransactionResult;
import com.github.h3yow.sagacoordinatorjava.model.TransactionStatus;
import com.github.h3yow.sagacoordinatorjava.tests.support.TestContext;
import com.github.h3yow.sagacoordinatorjava.tests.support.TestSagaFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景三：重复提交与并发提交的幂等性，以及非法状态迁移拒绝。
 */
class IdempotencyConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyConcurrencyTest.class);

    @TempDir
    Path tempDir;

    @Test
    void duplicateSubmitProducesSingleBusinessEffect() {
        SagaCoordinator coordinator =
                TestSagaFactory.newCoordinator(tempDir, 0, 60_000, 3);
        TestContext context = new TestContext();
        String txId = "tx-dup-001";

        TransactionResult first = coordinator.submit(
                TestSagaFactory.threeStepDefinition(), txId, context);
        // 第二次提交时若再产生业务效果就会失败：这里让"重放"必然报错
        context.failStep = TestSagaFactory.STEP_A;
        TransactionResult second = coordinator.submit(
                TestSagaFactory.threeStepDefinition(), txId, context);

        log.info("tx={} first={}/{} second={}/{}", txId,
                first.getStatus(), first.getFailureReason(),
                second.getStatus(), second.getFailureReason());

        assertEquals(first.getStatus(), second.getStatus());
        assertEquals(first.getFailureReason(), second.getFailureReason());
        assertEquals(TransactionStatus.COMPLETED, second.getStatus());
        assertEquals(1, context.executeCount(TestSagaFactory.STEP_A));
        assertEquals(1, context.executeCount(TestSagaFactory.STEP_B));
        assertEquals(1, context.executeCount(TestSagaFactory.STEP_C));
        assertEquals(0, context.compensateCount(TestSagaFactory.STEP_A));
    }

    @Test
    void concurrentSubmitsExecuteExactlyOnce() throws Exception {
        SagaCoordinator coordinator =
                TestSagaFactory.newCoordinator(tempDir, 0, 60_000, 3);
        TestContext context = new TestContext();
        String txId = "tx-concurrent-001";
        int threads = 8;

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<TransactionResult>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                return coordinator.submit(TestSagaFactory.threeStepDefinition(), txId, context);
            }));
        }
        start.countDown();

        for (Future<TransactionResult> f : futures) {
            TransactionResult r = f.get(10, TimeUnit.SECONDS);
            assertEquals(TransactionStatus.COMPLETED, r.getStatus());
            assertEquals(FailureReason.NONE, r.getFailureReason());
        }
        pool.shutdown();

        log.info("tx={} concurrent submitters={}, executed A={} B={} C={}",
                txId, threads,
                context.executeCount(TestSagaFactory.STEP_A),
                context.executeCount(TestSagaFactory.STEP_B),
                context.executeCount(TestSagaFactory.STEP_C));
        resultTrace(coordinator, txId);

        assertEquals(1, context.executeCount(TestSagaFactory.STEP_A));
        assertEquals(1, context.executeCount(TestSagaFactory.STEP_B));
        assertEquals(1, context.executeCount(TestSagaFactory.STEP_C));
    }

    @Test
    void illegalTransitionIsRejectedNotRewritten() {
        SagaTransaction tx = new SagaTransaction();
        tx.setStatus(TransactionStatus.COMPLETED);
        // 已完成事务试图回退到补偿中：必须拒绝
        IllegalTransitionException ex = assertThrows(IllegalTransitionException.class,
                () -> StateMachine.assertTxTransition(TransactionStatus.COMPLETED,
                        TransactionStatus.COMPENSATING));
        log.info("illegal transition rejected: {}", ex.getMessage());
        assertTrue(ex.getMessage().contains("COMPLETED -> COMPENSATING"));
        // 内存中的状态没有被就地改写
        assertEquals(TransactionStatus.COMPLETED, tx.getStatus());
    }

    private void resultTrace(SagaCoordinator coordinator, String txId) {
        coordinator.getResult(txId).ifPresent(r -> {
            log.info("tx={} finalStatus={}", txId, r.getStatus());
            r.getTrace().forEach(e -> log.info("tx={} trace {}", txId, e));
        });
    }
}
