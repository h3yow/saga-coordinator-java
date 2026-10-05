package com.github.h3yow.sagacoordinatorjava.tests;

import com.github.h3yow.sagacoordinatorjava.coordinator.ConcurrencyLimitExceededException;
import com.github.h3yow.sagacoordinatorjava.coordinator.SagaCoordinator;
import com.github.h3yow.sagacoordinatorjava.model.TransactionResult;
import com.github.h3yow.sagacoordinatorjava.model.TransactionStatus;
import com.github.h3yow.sagacoordinatorjava.tests.support.TestContext;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 容量约束：同时在推进的事务数有上限，超限请求按规则排队或拒绝；
 * 个别事务挂起（参与者长时间不返回）不拖累其它事务；
 * 挂起事务仍按超时规则收敛并释放容量。
 */
class ConcurrencyLimitTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyLimitTest.class);

    @TempDir
    Path tempDir;

    /** 第一步被门闩卡住、无视中断的上下文：模拟参与者调用长时间不返回。 */
    static class StuckContext extends TestContext {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch gate = new CountDownLatch(1);

        @Override
        public void execute(String step) {
            if (TestSagaFactory.STEP_A.equals(step)) {
                entered.countDown();
                while (gate.getCount() > 0) {
                    try {
                        gate.await(60, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        // 参与者不响应取消
                    }
                }
            }
            super.execute(step);
        }
    }

    @Test
    void excessSubmissionIsRejectedWhenLimitReached() throws Exception {
        // 上限 1，排队 200ms 后拒绝
        SagaCoordinator coordinator = TestSagaFactory.newCoordinator(
                tempDir, 0, 600_000, 3, 1, 200);
        StuckContext stuck = new StuckContext();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        pool.submit(() -> coordinator.submit(
                TestSagaFactory.threeStepDefinition(), "tx-cap-stuck-001", stuck));
        assertTrue(stuck.entered.await(10, TimeUnit.SECONDS));

        // 容量被挂起事务占满：新提交在排队超时后被拒绝
        long start = System.nanoTime();
        ConcurrencyLimitExceededException rejected = null;
        try {
            coordinator.submit(TestSagaFactory.threeStepDefinition(),
                    "tx-cap-overflow-001", new TestContext());
        } catch (ConcurrencyLimitExceededException e) {
            rejected = e;
        }
        long waitedMs = (System.nanoTime() - start) / 1_000_000;
        log.info("tx=tx-cap-overflow-001 rejected after {}ms: {}", waitedMs,
                rejected != null ? rejected.getMessage() : "<not rejected>");
        assertNotNull(rejected, "submission beyond capacity must be rejected");
        assertTrue(waitedMs >= 150, "rejection should follow the queue timeout rule");

        // 挂起事务解除后容量释放，同一事务可正常提交
        stuck.gate.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        TransactionResult result = coordinator.submit(
                TestSagaFactory.threeStepDefinition(), "tx-cap-overflow-001", new TestContext());
        assertEquals(TransactionStatus.COMPLETED, result.getStatus());
        log.info("tx=tx-cap-overflow-001 completed after capacity freed");
    }

    @Test
    void excessSubmissionQueuesAndProceedsWhenCapacityFrees() throws Exception {
        // 上限 1，排队窗口 5s；第一个事务 600ms 后让出容量
        SagaCoordinator coordinator = TestSagaFactory.newCoordinator(
                tempDir, 0, 600_000, 3, 1, 5_000);
        TestContext slow = new TestContext();
        slow.slowStep = TestSagaFactory.STEP_A;
        slow.slowMillis = 600;

        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<TransactionResult> first = pool.submit(() -> coordinator.submit(
                TestSagaFactory.threeStepDefinition(), "tx-cap-slow-001", slow));
        // 等第一个事务占住容量后再提交第二个
        Thread.sleep(150);
        long start = System.nanoTime();
        Future<TransactionResult> second = pool.submit(() -> coordinator.submit(
                TestSagaFactory.threeStepDefinition(), "tx-cap-queued-001", new TestContext()));

        TransactionResult r1 = first.get(10, TimeUnit.SECONDS);
        TransactionResult r2 = second.get(10, TimeUnit.SECONDS);
        long secondTotalMs = (System.nanoTime() - start) / 1_000_000;
        log.info("tx=tx-cap-queued-001 queued then completed, wall={}ms", secondTotalMs);
        assertEquals(TransactionStatus.COMPLETED, r1.getStatus());
        assertEquals(TransactionStatus.COMPLETED, r2.getStatus());
        assertTrue(secondTotalMs >= 400,
                "second transaction must have queued behind the first");
        pool.shutdown();
    }

    @Test
    void stuckTransactionDoesNotDragOthers() throws Exception {
        // 上限 2：一个挂起事务占住一份容量，其它事务照常提交与收敛
        SagaCoordinator coordinator = TestSagaFactory.newCoordinator(
                tempDir, 0, 600_000, 3, 2, 200);
        StuckContext stuck = new StuckContext();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        pool.submit(() -> coordinator.submit(
                TestSagaFactory.threeStepDefinition(), "tx-iso-stuck-001", stuck));
        assertTrue(stuck.entered.await(10, TimeUnit.SECONDS));

        long start = System.nanoTime();
        TransactionResult healthy = coordinator.submit(
                TestSagaFactory.threeStepDefinition(), "tx-iso-healthy-001", new TestContext());
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        log.info("tx=tx-iso-healthy-001 completed in {}ms while tx=tx-iso-stuck-001 is stuck",
                elapsedMs);
        assertEquals(TransactionStatus.COMPLETED, healthy.getStatus());
        assertTrue(elapsedMs < 5_000, "healthy transaction must not be dragged by the stuck one");
        assertEquals(TransactionStatus.RUNNING,
                coordinator.getResult("tx-iso-stuck-001").orElseThrow().getStatus());

        stuck.gate.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(TransactionStatus.COMPLETED,
                coordinator.getResult("tx-iso-stuck-001").orElseThrow().getStatus());
    }

    @Test
    void stuckTransactionIsConvergedByTimeoutAndFreesCapacity() throws Exception {
        // 上限 1，单步超时 300ms：挂起步骤被判超时 → 补偿收敛 → 容量释放
        SagaCoordinator coordinator = TestSagaFactory.newCoordinator(
                tempDir, 300, 600_000, 3, 1, 5_000);
        StuckContext stuck = new StuckContext();
        String stuckId = "tx-cap-timeout-001";

        // 挂起事务在后台推进：步骤超时后协调器线程继续补偿并收敛，
        // 其提交调用本身会等到收敛完成才返回
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<TransactionResult> stuckFuture = pool.submit(() -> coordinator.submit(
                TestSagaFactory.threeStepDefinition(), stuckId, stuck));
        assertTrue(stuck.entered.await(10, TimeUnit.SECONDS));

        TransactionResult stuckResult = stuckFuture.get(15, TimeUnit.SECONDS);
        log.info("tx={} converged to {} reason={} detail={}",
                stuckId, stuckResult.getStatus(), stuckResult.getFailureReason(),
                stuckResult.getFailureDetail());
        stuckResult.getTrace().forEach(e -> log.info("tx={} trace {}", stuckId, e));
        assertEquals(TransactionStatus.COMPENSATED, stuckResult.getStatus());
        assertEquals(com.github.h3yow.sagacoordinatorjava.model.FailureReason.STEP_TIMEOUT,
                stuckResult.getFailureReason());

        // 容量已释放：后续事务正常完成
        TransactionResult next = coordinator.submit(
                TestSagaFactory.threeStepDefinition(), "tx-cap-after-001", new TestContext());
        assertEquals(TransactionStatus.COMPLETED, next.getStatus());
        log.info("tx=tx-cap-after-001 completed after stuck transaction converged");

        stuck.gate.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
    }

    @Test
    void rejectionDoesNotCorruptExistingTransaction() throws Exception {
        SagaCoordinator coordinator = TestSagaFactory.newCoordinator(
                tempDir, 0, 600_000, 3, 1, 100);
        StuckContext stuck = new StuckContext();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<TransactionResult> stuckFuture = pool.submit(() -> coordinator.submit(
                TestSagaFactory.threeStepDefinition(), "tx-cap-keep-001", stuck));
        assertTrue(stuck.entered.await(10, TimeUnit.SECONDS));

        // 被拒绝的请求不能影响在途事务
        for (int i = 0; i < 3; i++) {
            try {
                coordinator.submit(TestSagaFactory.threeStepDefinition(),
                        "tx-cap-rej-" + i, new TestContext());
            } catch (ConcurrencyLimitExceededException e) {
                log.info("tx=tx-cap-rej-{} rejected as expected", i);
            }
        }
        assertEquals(TransactionStatus.RUNNING,
                coordinator.getResult("tx-cap-keep-001").orElseThrow().getStatus());

        stuck.gate.countDown();
        TransactionResult result = stuckFuture.get(10, TimeUnit.SECONDS);
        assertEquals(TransactionStatus.COMPLETED, result.getStatus());
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        // 被拒绝的事务从未产生业务效果，也未落盘
        assertTrue(coordinator.getResult("tx-cap-rej-0").isEmpty());
    }
}
