package com.github.h3yow.sagacoordinatorjava.tests;

import com.github.h3yow.sagacoordinatorjava.coordinator.CapacityExceededException;
import com.github.h3yow.sagacoordinatorjava.coordinator.ConcurrencyLimiter;
import com.github.h3yow.sagacoordinatorjava.coordinator.RecoveryService;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 隔离与容量：
 * 1) 某个参与者长时间不返回时，其它事务的提交与收敛不被拖住；
 * 2) 被卡住的事务由超时规则自动收敛，不会无限挂着；
 * 3) 同时推进事务数有上限，REJECT 策略超额快速拒绝、QUEUE 策略排队等到空位；
 * 4) 巡检遇到正被占住的事务锁时跳过，不影响其它超时事务的收敛。
 */
class IsolationConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(IsolationConcurrencyTest.class);

    @TempDir
    Path tempDir;

    @Test
    void hungTransactionDoesNotBlockOthersAndTimesOutByItself() throws Exception {
        // 单步超时 300ms；事务 A 参与者永久挂起
        SagaCoordinator coordinator =
                TestSagaFactory.newCoordinator(tempDir, 300, 60_000, 2);

        TestContext hungContext = new TestContext();
        hungContext.hangStep = TestSagaFactory.STEP_A;
        ExecutorService pool = Executors.newCachedThreadPool();
        Future<TransactionResult> hungFuture = pool.submit(() -> coordinator.submit(
                TestSagaFactory.threeStepDefinition(), "tx-hung-001", hungContext));
        assertTrue(hungContext.hangEntered.await(10, TimeUnit.SECONDS),
                "hung step never entered");
        log.info("tx=tx-hung-001 participant now hangs forever (ignoring interrupts)");

        // 另一个事务必须能正常提交完成，且耗时远小于"卡死"时长
        long start = System.currentTimeMillis();
        TestContext otherContext = new TestContext();
        TransactionResult other = coordinator.submit(
                TestSagaFactory.threeStepDefinition(), "tx-healthy-001", otherContext);
        long elapsed = System.currentTimeMillis() - start;
        log.info("tx=tx-healthy-001 finished in {}ms status={} while tx-hung-001 is stuck",
                elapsed, other.getStatus());
        assertEquals(TransactionStatus.COMPLETED, other.getStatus());
        assertTrue(elapsed < 5_000, "healthy tx must not be blocked by hung tx, took " + elapsed);

        // 被卡住的事务按单步超时自动收敛（不再无限挂着）
        TransactionResult hung = hungFuture.get(10, TimeUnit.SECONDS);
        log.info("tx=tx-hung-001 converged by timeout: status={} reason={}",
                hung.getStatus(), hung.getFailureReason());
        hung.getTrace().forEach(e -> log.info("tx=tx-hung-001 trace {}", e));
        assertEquals(TransactionStatus.COMPENSATED, hung.getStatus());
        // 让参与者调用最终返回，泄漏的执行线程干净退出
        hungContext.hangGate.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS), "hung task should finish");
    }

    @Test
    void rejectPolicyRefusesBeyondLimit() throws Exception {
        SagaCoordinator coordinator = TestSagaFactory.newCoordinator(
                tempDir, 0, 60_000, 3,
                ConcurrencyLimiter.of(2, ConcurrencyLimiter.Policy.REJECT, 0));

        TestContext hangA = new TestContext();
        hangA.hangStep = TestSagaFactory.STEP_A;
        TestContext hangB = new TestContext();
        hangB.hangStep = TestSagaFactory.STEP_A;

        ExecutorService pool = Executors.newFixedThreadPool(3);
        pool.submit(() -> coordinator.submit(
                TestSagaFactory.threeStepDefinition(), "tx-cap-001", hangA));
        pool.submit(() -> coordinator.submit(
                TestSagaFactory.threeStepDefinition(), "tx-cap-002", hangB));
        assertTrue(hangA.hangEntered.await(10, TimeUnit.SECONDS));
        assertTrue(hangB.hangEntered.await(10, TimeUnit.SECONDS));
        log.info("two transactions now occupy the limit of 2");

        // 第三个立即被拒绝，不占线程、不等待
        long start = System.currentTimeMillis();
        TestContext third = new TestContext();
        try {
            coordinator.submit(TestSagaFactory.threeStepDefinition(), "tx-cap-003", third);
            throw new AssertionError("expected CapacityExceededException");
        } catch (CapacityExceededException e) {
            long waited = System.currentTimeMillis() - start;
            log.info("third tx rejected in {}ms: {}", waited, e.getMessage());
            assertTrue(waited < 1_000, "REJECT must be immediate, waited " + waited);
            assertEquals(0, third.executeCount(TestSagaFactory.STEP_A),
                    "rejected tx must have no business effect");
        } finally {
            // 让前两个参与者"最终返回"，线程干净退出（不影响上面的拒绝断言）
            hangA.hangGate.countDown();
            hangB.hangGate.countDown();
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS), "hung tasks should finish");
    }

    @Test
    void queuePolicyWaitsUntilSlotFrees() throws Exception {
        SagaCoordinator coordinator = TestSagaFactory.newCoordinator(
                tempDir, 0, 60_000, 3,
                ConcurrencyLimiter.of(1, ConcurrencyLimiter.Policy.QUEUE, 30_000));

        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        TestContext firstCtx = new TestContext() {
            @Override
            public void execute(String step) {
                if (TestSagaFactory.STEP_A.equals(step)) {
                    firstStarted.countDown();
                    try {
                        releaseFirst.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                super.execute(step);
            }
        };

        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<TransactionResult> f1 = pool.submit(() -> coordinator.submit(
                TestSagaFactory.threeStepDefinition(), "tx-queue-001", firstCtx));
        assertTrue(firstStarted.await(10, TimeUnit.SECONDS));

        // 第二个在容量满时排队，不立即失败
        TestContext secondCtx = new TestContext();
        Future<TransactionResult> f2 = pool.submit(() -> coordinator.submit(
                TestSagaFactory.threeStepDefinition(), "tx-queue-002", secondCtx));
        Thread.sleep(500);
        assertEquals(0, secondCtx.executeCount(TestSagaFactory.STEP_A),
                "queued tx must not start while the only permit is held");
        log.info("tx=tx-queue-002 is queued, not started yet");

        // 第一个释放许可后，排队事务得到推进
        releaseFirst.countDown();
        assertEquals(TransactionStatus.COMPLETED, f1.get(10, TimeUnit.SECONDS).getStatus());
        TransactionResult second = f2.get(10, TimeUnit.SECONDS);
        log.info("tx=tx-queue-002 proceeded after slot freed: status={}", second.getStatus());
        assertEquals(TransactionStatus.COMPLETED, second.getStatus());
        pool.shutdownNow();
    }

    @Test
    void sweepSkipsBusyTxAndStillConvergesOtherTimedOutTx() throws Exception {
        // 单一协调器：提交线程正持有 tx-busy 的分段锁并挂在参与者调用里
        // （stepTimeout=0 表示该调用不设超时），巡检线程对它 tryLock 必然失败、跳过本轮；
        // 与此同时，另一条未被占锁且 deadline 已过的事务必须仍被正常收敛。
        SagaCoordinator coordinator = TestSagaFactory.newCoordinator(tempDir, 0, 60_000, 3);
        RecoveryService recovery = new RecoveryService(
                new com.github.h3yow.sagacoordinatorjava.store.FileTransactionStore(
                        tempDir, new com.fasterxml.jackson.databind.ObjectMapper()),
                coordinator);

        TestContext busyCtx = new TestContext() {
            @Override
            public void execute(String step) {
                if (TestSagaFactory.STEP_A.equals(step)) {
                    try {
                        Thread.sleep(30_000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                super.execute(step);
            }
        };
        String busyTxId = "tx-busy-sweep-001";
        ExecutorService pool = Executors.newSingleThreadExecutor();
        pool.submit(() -> coordinator.submit(
                TestSagaFactory.threeStepDefinition(), busyTxId, busyCtx));
        waitUntilPresent(coordinator, busyTxId);
        forceDeadlineExpired(coordinator, busyTxId);
        log.info("tx={} is RUNNING with its per-tx lock held by the blocked submit thread",
                busyTxId);

        // 另一条事务：未被占锁、deadline 已过，需要被本轮巡检收敛
        String otherTxId = "tx-sweep-other-001";
        persistTimedOutRunning(coordinator, otherTxId);

        long start = System.currentTimeMillis();
        recovery.sweepTimeouts(System.currentTimeMillis());
        long elapsed = System.currentTimeMillis() - start;
        log.info("sweep finished in {}ms (busy tx skipped, other tx converged)", elapsed);

        TransactionResult other = coordinator.getResult(otherTxId).orElseThrow();
        log.info("tx={} swept status={} reason={}",
                otherTxId, other.getStatus(), other.getFailureReason());
        assertEquals(TransactionStatus.TIMED_OUT_COMPENSATED, other.getStatus());
        assertTrue(elapsed < 10_000, "sweep must not block on the busy tx, took " + elapsed);

        // 被占住的事务本轮被跳过，仍是 RUNNING（下一轮锁空出来再收敛）
        TransactionStatus busyStatus = coordinator.getResult(busyTxId).orElseThrow().getStatus();
        log.info("tx={} remains {} after sweep (its lock was busy)", busyTxId, busyStatus);
        assertEquals(TransactionStatus.RUNNING, busyStatus);
        pool.shutdownNow();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS), "busy task should end on interrupt");
    }

    @Test
    void overviewQueryDoesNotBlockOnInFlightTransactions() throws Exception {
        SagaCoordinator coordinator =
                TestSagaFactory.newCoordinator(tempDir, 0, 60_000, 3);
        var queryService = new com.github.h3yow.sagacoordinatorjava.coordinator
                .TransactionQueryService(
                new com.github.h3yow.sagacoordinatorjava.store.FileTransactionStore(
                        tempDir, new com.fasterxml.jackson.databind.ObjectMapper()));

        // 先放一批已完成事务
        for (int i = 0; i < 10; i++) {
            coordinator.submit(TestSagaFactory.threeStepDefinition(),
                    "tx-qdone-" + i, new TestContext());
        }
        // 再起一个永久挂起、锁与许可都被占住的事务
        TestContext hung = new TestContext();
        hung.hangStep = TestSagaFactory.STEP_A;
        ExecutorService pool = Executors.newSingleThreadExecutor();
        pool.submit(() -> coordinator.submit(
                TestSagaFactory.threeStepDefinition(), "tx-qhung-001", hung));
        assertTrue(hung.hangEntered.await(10, TimeUnit.SECONDS));

        long start = System.currentTimeMillis();
        var runningPage = queryService.query(
                new com.github.h3yow.sagacoordinatorjava.model.TransactionFilter(
                        TransactionStatus.RUNNING, null, 0, 50));
        var completedPage = queryService.query(
                new com.github.h3yow.sagacoordinatorjava.model.TransactionFilter(
                        TransactionStatus.COMPLETED, null, 0, 50));
        long elapsed = System.currentTimeMillis() - start;

        log.info("two paged queries while tx in-flight took {}ms; running={} completed={}",
                elapsed, runningPage.getTotalItems(), completedPage.getTotalItems());
        assertEquals(1, runningPage.getTotalItems());
        assertEquals("tx-qhung-001", runningPage.getItems().get(0).getTransactionId());
        assertEquals(10, completedPage.getTotalItems());
        assertTrue(elapsed < 3_000, "query must not wait on in-flight tx locks, took " + elapsed);

        hung.hangGate.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
    }

    private void persistTimedOutRunning(SagaCoordinator coordinator, String txId) {
        com.github.h3yow.sagacoordinatorjava.model.SagaTransaction tx =
                new com.github.h3yow.sagacoordinatorjava.model.SagaTransaction();
        tx.setTransactionId(txId);
        tx.setSagaType(TestSagaFactory.SAGA_TYPE);
        tx.setStatus(TransactionStatus.RUNNING);
        tx.setFailureReason(com.github.h3yow.sagacoordinatorjava.model.FailureReason.NONE);
        tx.setCurrentStepIndex(0);
        long now = System.currentTimeMillis();
        tx.setCreatedAt(now - 10_000);
        tx.setUpdatedAt(now - 10_000);
        tx.setDeadlineAt(now - 1_000); // 已过期
        new com.github.h3yow.sagacoordinatorjava.store.FileTransactionStore(
                tempDir, new com.fasterxml.jackson.databind.ObjectMapper()).save(tx);
        // 收敛需要 Saga 定义（无已完成步骤，直接到 TIMED_OUT_COMPENSATED）
        coordinator.register(TestSagaFactory.threeStepDefinition(), TestContext::new);
    }

    private void waitUntilPresent(SagaCoordinator coordinator, String txId) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (coordinator.getResult(txId).isPresent()) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        throw new AssertionError("tx " + txId + " never persisted");
    }

    private void forceDeadlineExpired(SagaCoordinator coordinator, String txId) {
        com.github.h3yow.sagacoordinatorjava.store.FileTransactionStore store =
                new com.github.h3yow.sagacoordinatorjava.store.FileTransactionStore(
                        tempDir, new com.fasterxml.jackson.databind.ObjectMapper());
        var tx = store.find(txId).orElseThrow();
        tx.setDeadlineAt(System.currentTimeMillis() - 1_000);
        store.save(tx);
    }
}
