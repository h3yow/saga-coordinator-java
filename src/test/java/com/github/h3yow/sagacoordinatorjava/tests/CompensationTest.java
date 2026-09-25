package com.github.h3yow.sagacoordinatorjava.tests;

import com.github.h3yow.sagacoordinatorjava.coordinator.SagaCoordinator;
import com.github.h3yow.sagacoordinatorjava.model.FailureReason;
import com.github.h3yow.sagacoordinatorjava.model.TransactionResult;
import com.github.h3yow.sagacoordinatorjava.model.TransactionStatus;
import com.github.h3yow.sagacoordinatorjava.tests.support.TestContext;
import com.github.h3yow.sagacoordinatorjava.tests.support.TestSagaFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景二：参与者步骤失败后按逆序补偿；补偿无法收敛时明确标记原因。
 */
class CompensationTest {

    private static final Logger log = LoggerFactory.getLogger(CompensationTest.class);

    @TempDir
    Path tempDir;

    @Test
    void failedStepTriggersReverseCompensation() {
        SagaCoordinator coordinator =
                TestSagaFactory.newCoordinator(tempDir, 0, 60_000, 3);
        TestContext context = new TestContext();
        context.failStep = TestSagaFactory.STEP_C; // 第三步失败，A、B 已完成
        String txId = "tx-compensate-001";

        TransactionResult result = coordinator.submit(
                TestSagaFactory.threeStepDefinition(), txId, context);

        log.info("tx={} finalStatus={} reason={} detail={}",
                result.getTransactionId(), result.getStatus(),
                result.getFailureReason(), result.getFailureDetail());
        result.getTrace().forEach(e -> log.info("tx={} trace {}", txId, e));

        assertEquals(TransactionStatus.COMPENSATED, result.getStatus());
        assertEquals(FailureReason.STEP_FAILED, result.getFailureReason());
        assertTrue(result.getFailureDetail().contains(TestSagaFactory.STEP_C));
        // 逆序补偿：B 先于 A；C 未产生业务效果，不补偿
        assertEquals(List.of(TestSagaFactory.STEP_B, TestSagaFactory.STEP_A),
                context.compensatedSteps);
        assertEquals(1, context.compensateCount(TestSagaFactory.STEP_B));
        assertEquals(1, context.compensateCount(TestSagaFactory.STEP_A));
        assertEquals(0, context.compensateCount(TestSagaFactory.STEP_C));
    }

    @Test
    void compensationFailureIsMarkedWithDistinctReason() {
        SagaCoordinator coordinator =
                TestSagaFactory.newCoordinator(tempDir, 0, 60_000, 2);
        TestContext context = new TestContext();
        context.failStep = TestSagaFactory.STEP_C;
        context.compensationFailStep = TestSagaFactory.STEP_B; // B 的补偿始终失败
        String txId = "tx-comp-fail-001";

        TransactionResult result = coordinator.submit(
                TestSagaFactory.threeStepDefinition(), txId, context);

        log.info("tx={} finalStatus={} reason={} detail={}",
                result.getTransactionId(), result.getStatus(),
                result.getFailureReason(), result.getFailureDetail());
        result.getTrace().forEach(e -> log.info("tx={} trace {}", txId, e));

        // 必须明确标记为补偿失败（待人工处理），不能静默丢弃
        assertEquals(TransactionStatus.COMPENSATION_FAILED, result.getStatus());
        assertEquals(FailureReason.COMPENSATION_FAILED, result.getFailureReason());
        assertTrue(result.getFailureDetail().contains(TestSagaFactory.STEP_B));
        // B 按最大重试次数尝试；A 因 B 未收敛而尚未补偿
        assertEquals(2, context.compensateCount(TestSagaFactory.STEP_B));
        assertEquals(0, context.compensateCount(TestSagaFactory.STEP_A));
    }
}
