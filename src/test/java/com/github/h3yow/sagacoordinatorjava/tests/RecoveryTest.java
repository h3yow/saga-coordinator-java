package com.github.h3yow.sagacoordinatorjava.tests;

import com.github.h3yow.sagacoordinatorjava.coordinator.SagaCoordinator;
import com.github.h3yow.sagacoordinatorjava.model.FailureReason;
import com.github.h3yow.sagacoordinatorjava.model.TransactionResult;
import com.github.h3yow.sagacoordinatorjava.model.TransactionStatus;
import com.github.h3yow.sagacoordinatorjava.tests.support.TestSagaFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 场景四：进程重启后从持久化状态继续推进；长时间无进展事务超时收敛。
 * 用"新建一个指向同一目录的协调器"模拟进程重启；
 * 用阻塞在第一步的动作模拟"崩溃发生在 EXECUTING 落盘之后、结果未知之前"。
 */
class RecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(RecoveryTest.class);

    @TempDir
    Path tempDir;

    /**
     * 第一步可被门闩阻塞的上下文，用来模拟执行中进程被 kill。
     * 门闩关闭时即使收到中断也永久等待——忠实模拟"旧进程已死、不再写盘"。
     */
    static class BlockingContext extends com.github.h3yow.sagacoordinatorjava
            .tests.support.TestContext {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch gate = new CountDownLatch(1);
        final AtomicInteger aCalls = new AtomicInteger();

        @Override
        public void execute(String step) {
            if (STEP_A.equals(step)) {
                aCalls.incrementAndGet();
                entered.countDown();
                // 循环等待：忽略中断，模拟崩溃后该线程永远不会再推进、落盘
                while (gate.getCount() > 0) {
                    try {
                        gate.await(60, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        // 故意吞掉中断并继续等待
                    }
                }
            }
            super.execute(step);
        }
    }

    static final String STEP_A = TestSagaFactory.STEP_A;

    @Test
    void completedTransactionSurvivesRestartWithSameConclusion() {
        var context = new com.github.h3yow.sagacoordinatorjava.tests.support.TestContext();
        SagaCoordinator first = TestSagaFactory.newCoordinator(tempDir, 0, 60_000, 3);
        String txId = "tx-restart-done-001";
        TransactionResult before = first.submit(
                TestSagaFactory.threeStepDefinition(), txId, context);
        assertEquals(TransactionStatus.COMPLETED, before.getStatus());

        // 模拟重启：全新协调器实例指向同一存储目录，结论一致且不重复执行业务
        SagaCoordinator second = TestSagaFactory.newCoordinator(tempDir, 0, 60_000, 3);
        var freshContext = new com.github.h3yow.sagacoordinatorjava.tests.support.TestContext();
        TransactionResult after = second.submit(
                TestSagaFactory.threeStepDefinition(), txId, freshContext);

        log.info("tx={} before={} after={} (fresh context executeCounts all zero: A={} B={} C={})",
                txId, before.getStatus(), after.getStatus(),
                freshContext.executeCount(STEP_A),
                freshContext.executeCount(TestSagaFactory.STEP_B),
                freshContext.executeCount(TestSagaFactory.STEP_C));
        after.getTrace().forEach(e -> log.info("tx={} trace {}", txId, e));

        assertEquals(TransactionStatus.COMPLETED, after.getStatus());
        assertEquals(FailureReason.NONE, after.getFailureReason());
        assertEquals(0, freshContext.executeCount(STEP_A));
    }

    @Test
    void inFlightStepIsResumedIdempotentlyAfterRestart() throws Exception {
        // --- 旧进程：第一步状态已落盘 EXECUTING，但业务结果未知，进程"崩溃" ---
        SagaCoordinator oldProcess = TestSagaFactory.newCoordinator(tempDir, 0, 60_000, 3);
        BlockingContext oldContext = new BlockingContext();
        String txId = "tx-crash-midstep-001";

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<?> stuck = pool.submit(() -> oldProcess.submit(
                TestSagaFactory.threeStepDefinition(), txId, oldContext));
        assertTrueAwait(oldContext.entered);
        // 磁盘上必须已经能看到 RUNNING + EXECUTING
        assertNotNull(oldProcess.getResult(txId).orElse(null));
        log.info("tx={} crashed while step A EXECUTING (persisted to disk)", txId);

        // --- 新进程：指向同一存储目录，重新注册定义后恢复 ---
        var newContext = new com.github.h3yow.sagacoordinatorjava.tests.support.TestContext();
        SagaCoordinator newProcess = TestSagaFactory.newCoordinator(tempDir, 0, 60_000, 3);
        TransactionResult result = newProcess.submit(
                TestSagaFactory.threeStepDefinition(), txId, newContext);

        log.info("tx={} resumed status={} reason={}",
                txId, result.getStatus(), result.getFailureReason());
        result.getTrace().forEach(e -> log.info("tx={} trace {}", txId, e));

        assertEquals(TransactionStatus.COMPLETED, result.getStatus());
        assertEquals(1, newContext.executeCount(STEP_A), "resumed step A must run exactly once");
        assertEquals(1, newContext.executeCount(TestSagaFactory.STEP_B));
        assertEquals(1, newContext.executeCount(TestSagaFactory.STEP_C));
        // 旧进程保持"崩溃"状态：阻塞线程被遗弃（其步骤执行线程为 daemon，JVM 退出时回收）
    }

    @Test
    void staleRunningTransactionIsSweptToTimedOutCompensated() throws Exception {
        // --- 旧进程：事务停在第一步 EXECUTING ---
        SagaCoordinator oldProcess = TestSagaFactory.newCoordinator(tempDir, 0, 60_000, 3);
        BlockingContext oldContext = new BlockingContext();
        String txId = "tx-stale-sweep-001";
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<?> stuck = pool.submit(() -> oldProcess.submit(
                TestSagaFactory.threeStepDefinition(), txId, oldContext));
        assertTrueAwait(oldContext.entered);

        // --- 新进程：指向同一存储目录，先重新注册定义，再按事务超时巡检收敛 ---
        SagaCoordinator newProcess = TestSagaFactory.newCoordinator(tempDir, 0, 60_000, 3);
        var newContext = new com.github.h3yow.sagacoordinatorjava.tests.support.TestContext();
        newProcess.register(TestSagaFactory.threeStepDefinition(), () -> newContext);
        newProcess.timeoutTransaction(txId);
        TransactionResult result = newProcess.getResult(txId).orElseThrow();

        log.info("tx={} swept status={} reason={} detail={}",
                txId, result.getStatus(), result.getFailureReason(), result.getFailureDetail());
        result.getTrace().forEach(e -> log.info("tx={} trace {}", txId, e));

        assertEquals(TransactionStatus.TIMED_OUT_COMPENSATED, result.getStatus());
        assertEquals(FailureReason.TRANSACTION_TIMEOUT, result.getFailureReason());
        // 崩溃时没有任何已完成步骤，所以无补偿动作——结论可区分且不臆造补偿
        assertEquals(0, oldContext.compensatedSteps.size());
        // 旧进程保持"崩溃"状态，不再被唤醒
    }

    @Test
    void compensationFailedIsTerminalAndStableAcrossRestart() {
        var runContext = new com.github.h3yow.sagacoordinatorjava.tests.support.TestContext();
        runContext.failStep = TestSagaFactory.STEP_C;
        runContext.compensationFailStep = TestSagaFactory.STEP_B;
        SagaCoordinator first = TestSagaFactory.newCoordinator(tempDir, 0, 60_000, 1);
        String txId = "tx-restart-compfailed-001";
        TransactionResult stuck = first.submit(
                TestSagaFactory.threeStepDefinition(), txId, runContext);
        assertEquals(TransactionStatus.COMPENSATION_FAILED, stuck.getStatus());

        // 重启后终态结论不漂移：COMPENSATION_FAILED 表示待人工处理，不会被自动重放
        SagaCoordinator second = TestSagaFactory.newCoordinator(tempDir, 0, 60_000, 3);
        TransactionResult again = second.submit(
                TestSagaFactory.threeStepDefinition(), txId,
                new com.github.h3yow.sagacoordinatorjava.tests.support.TestContext());
        log.info("tx={} before={} after={} (manual intervention required)",
                txId, stuck.getStatus(), again.getStatus());
        assertEquals(TransactionStatus.COMPENSATION_FAILED, again.getStatus());
        assertEquals(FailureReason.COMPENSATION_FAILED, again.getFailureReason());
    }

    private static void assertTrueAwait(CountDownLatch latch) throws InterruptedException {
        org.junit.jupiter.api.Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS),
                "blocking step was never entered");
    }
}
