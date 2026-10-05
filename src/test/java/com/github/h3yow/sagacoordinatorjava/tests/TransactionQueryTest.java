package com.github.h3yow.sagacoordinatorjava.tests;

import com.github.h3yow.sagacoordinatorjava.coordinator.SagaCoordinator;
import com.github.h3yow.sagacoordinatorjava.coordinator.TransactionQueryService;
import com.github.h3yow.sagacoordinatorjava.model.PageResult;
import com.github.h3yow.sagacoordinatorjava.model.SagaDefinition;
import com.github.h3yow.sagacoordinatorjava.model.TransactionStatus;
import com.github.h3yow.sagacoordinatorjava.model.TransactionSummary;
import com.github.h3yow.sagacoordinatorjava.store.FileTransactionStore;
import com.github.h3yow.sagacoordinatorjava.tests.support.TestContext;
import com.github.h3yow.sagacoordinatorjava.tests.support.TestSagaFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 事务总览查询：按状态 / 业务类型筛选、稳定分页、摘要字段完整；
 * 上千条事务时分页稳定；查询不阻塞正在推进的事务。
 */
class TransactionQueryTest {

    private static final Logger log = LoggerFactory.getLogger(TransactionQueryTest.class);

    private static final String TYPE_B = "refund-saga";

    @TempDir
    Path tempDir;

    private TransactionQueryService newQueryService() {
        return new TransactionQueryService(new FileTransactionStore(tempDir, new ObjectMapper()));
    }

    private static SagaDefinition<TestContext> twoStepTypeB() {
        SagaDefinition<TestContext> def = new SagaDefinition<>(TYPE_B);
        def.step("inspect-refund", ctx -> ctx.execute("inspect-refund"),
                ctx -> ctx.compensate("inspect-refund"))
           .step("issue-refund", ctx -> ctx.execute("issue-refund"),
                ctx -> ctx.compensate("issue-refund"));
        return def;
    }

    /** 造一批混合状态、混合类型的事务，返回各状态数量。 */
    private SagaCoordinator seedMixedTransactions(int completed, int compensated,
                                                  int compensationFailed, int typeBCompleted) {
        SagaCoordinator coordinator = TestSagaFactory.newCoordinator(tempDir, 0, 600_000, 1);
        for (int i = 0; i < completed; i++) {
            coordinator.submit(TestSagaFactory.threeStepDefinition(),
                    String.format("tx-q-done-%03d", i), new TestContext());
        }
        for (int i = 0; i < compensated; i++) {
            TestContext ctx = new TestContext();
            ctx.failStep = TestSagaFactory.STEP_C;
            coordinator.submit(TestSagaFactory.threeStepDefinition(),
                    String.format("tx-q-comp-%03d", i), ctx);
        }
        for (int i = 0; i < compensationFailed; i++) {
            TestContext ctx = new TestContext();
            ctx.failStep = TestSagaFactory.STEP_C;
            ctx.compensationFailStep = TestSagaFactory.STEP_B;
            coordinator.submit(TestSagaFactory.threeStepDefinition(),
                    String.format("tx-q-stuck-%03d", i), ctx);
        }
        for (int i = 0; i < typeBCompleted; i++) {
            coordinator.submit(twoStepTypeB(),
                    String.format("tx-q-typeb-%03d", i), new TestContext());
        }
        return coordinator;
    }

    @Test
    void filterByStatusAndSagaType() {
        seedMixedTransactions(5, 4, 3, 2);
        TransactionQueryService query = newQueryService();

        PageResult<TransactionSummary> all = query.query(null, null, 0, 100);
        assertEquals(14, all.getTotalElements());
        assertEquals(14, all.getItems().size());

        PageResult<TransactionSummary> completed =
                query.query(TransactionStatus.COMPLETED, null, 0, 100);
        assertEquals(7, completed.getTotalElements()); // 5 typeA + 2 typeB
        assertTrue(completed.getItems().stream()
                .allMatch(s -> s.getStatus() == TransactionStatus.COMPLETED));

        PageResult<TransactionSummary> stuck =
                query.query(TransactionStatus.COMPENSATION_FAILED, null, 0, 100);
        assertEquals(3, stuck.getTotalElements());
        TransactionSummary sample = stuck.getItems().get(0);
        log.info("summary tx={} status={} reason={} detail={} updatedAt={}",
                sample.getTransactionId(), sample.getStatus(),
                sample.getFailureReason(), sample.getFailureDetail(), sample.getUpdatedAt());
        assertEquals(com.github.h3yow.sagacoordinatorjava.model.FailureReason.COMPENSATION_FAILED,
                sample.getFailureReason());
        assertNotNull(sample.getFailureDetail());
        assertTrue(sample.getUpdatedAt() >= sample.getCreatedAt());

        PageResult<TransactionSummary> typeB = query.query(null, TYPE_B, 0, 100);
        assertEquals(2, typeB.getTotalElements());
        assertTrue(typeB.getItems().stream().allMatch(s -> TYPE_B.equals(s.getSagaType())));

        PageResult<TransactionSummary> typeBCompleted =
                query.query(TransactionStatus.COMPLETED, TYPE_B, 0, 100);
        assertEquals(2, typeBCompleted.getTotalElements());

        PageResult<TransactionSummary> typeACompensated = query.query(
                TransactionStatus.COMPENSATED, TestSagaFactory.SAGA_TYPE, 0, 100);
        assertEquals(4, typeACompensated.getTotalElements());
    }

    @Test
    void paginationIsStableAndComplete() {
        seedMixedTransactions(10, 7, 5, 3);
        TransactionQueryService query = newQueryService();

        int size = 4;
        Set<String> seen = new HashSet<>();
        long previousUpdatedAt = Long.MAX_VALUE;
        String previousId = null;
        int page = 0;
        while (true) {
            PageResult<TransactionSummary> result = query.query(null, null, page, size);
            for (TransactionSummary s : result.getItems()) {
                assertTrue(seen.add(s.getTransactionId()),
                        "duplicate across pages: " + s.getTransactionId());
                // 排序：updatedAt 倒序，相同则 transactionId 升序
                int cmp = Long.compare(previousUpdatedAt, s.getUpdatedAt());
                assertTrue(cmp > 0 || (cmp == 0 && previousId != null
                                && previousId.compareTo(s.getTransactionId()) < 0),
                        "ordering violated at " + s.getTransactionId());
                previousUpdatedAt = s.getUpdatedAt();
                previousId = s.getTransactionId();
            }
            if (result.getItems().size() < size) {
                assertEquals(25, result.getTotalElements());
                assertEquals(7, result.getTotalPages());
                break;
            }
            page++;
        }
        assertEquals(25, seen.size());
        log.info("paged through {} transactions in {} pages of {}", seen.size(), page + 1, size);
    }

    @Test
    void thousandsOfTransactionsPageStably() {
        SagaCoordinator coordinator = TestSagaFactory.newCoordinator(tempDir, 0, 600_000, 1);
        int total = 1200;
        for (int i = 0; i < total; i++) {
            coordinator.submit(TestSagaFactory.threeStepDefinition(),
                    String.format("tx-bulk-%04d", i), new TestContext());
        }
        TransactionQueryService query = newQueryService();

        long start = System.nanoTime();
        Set<String> seen = new HashSet<>();
        int size = 100;
        for (int page = 0; page * size < total; page++) {
            PageResult<TransactionSummary> result =
                    query.query(TransactionStatus.COMPLETED, null, page, size);
            assertEquals(size, result.getItems().size());
            result.getItems().forEach(s -> assertTrue(seen.add(s.getTransactionId())));
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        log.info("paged {} transactions (pageSize={}) in {}ms", seen.size(), size, elapsedMs);
        assertEquals(total, seen.size());

        PageResult<TransactionSummary> first = query.query(null, null, 0, 20);
        assertEquals(total, first.getTotalElements());
        assertEquals(total / 20, first.getTotalPages());
    }

    @Test
    void queryDoesNotInterfereWithInFlightTransaction() throws Exception {
        SagaCoordinator coordinator = TestSagaFactory.newCoordinator(tempDir, 0, 600_000, 3);
        coordinator.submit(TestSagaFactory.threeStepDefinition(), "tx-q-idle-001", new TestContext());

        // 一个事务卡在第一步（门闩阻塞、忽略中断，模拟参与者长时间不返回）
        BlockingContext blocking = new BlockingContext();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        pool.submit(() -> coordinator.submit(
                TestSagaFactory.threeStepDefinition(), "tx-q-running-001", blocking));
        assertTrue(blocking.entered.await(10, TimeUnit.SECONDS));

        // 查询应立即返回，且能看到 RUNNING 中的事务摘要
        TransactionQueryService query = newQueryService();
        long start = System.nanoTime();
        PageResult<TransactionSummary> running =
                query.query(TransactionStatus.RUNNING, null, 0, 10);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        log.info("query during in-flight tx returned in {}ms: total={}", elapsedMs,
                running.getTotalElements());
        assertEquals(1, running.getTotalElements());
        assertEquals("tx-q-running-001", running.getItems().get(0).getTransactionId());
        assertTrue(elapsedMs < 5000, "query must not be blocked by in-flight transaction");

        blocking.gate.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        PageResult<TransactionSummary> after =
                query.query(TransactionStatus.COMPLETED, null, 0, 10);
        assertEquals(2, after.getTotalElements());
    }

    /** 门闩阻塞、忽略中断的第一步，模拟参与者调用长时间不返回。 */
    static class BlockingContext extends TestContext {
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
                        // 忽略中断：参与者不响应取消
                    }
                }
            }
            super.execute(step);
        }
    }
}
