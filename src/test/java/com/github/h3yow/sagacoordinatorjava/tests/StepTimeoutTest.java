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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景补充：单步执行超时后按逆序补偿，原因与业务失败可区分（STEP_TIMEOUT）。
 */
class StepTimeoutTest {

    private static final Logger log = LoggerFactory.getLogger(StepTimeoutTest.class);

    @TempDir
    Path tempDir;

    @Test
    void slowStepTimesOutAndPriorStepsAreCompensated() {
        SagaCoordinator coordinator =
                TestSagaFactory.newCoordinator(tempDir, 300, 60_000, 2);
        TestContext context = new TestContext();
        // 第二步卡住超过 300ms 的单步超时；第一步已完成需要补偿
        context.slowStep = TestSagaFactory.STEP_B;
        context.slowMillis = 5_000;
        String txId = "tx-step-timeout-001";

        TransactionResult result = coordinator.submit(
                TestSagaFactory.threeStepDefinition(), txId, context);

        log.info("tx={} finalStatus={} reason={} detail={}",
                result.getTransactionId(), result.getStatus(),
                result.getFailureReason(), result.getFailureDetail());
        result.getTrace().forEach(e -> log.info("tx={} trace {}", txId, e));

        assertEquals(TransactionStatus.COMPENSATED, result.getStatus());
        assertEquals(FailureReason.STEP_TIMEOUT, result.getFailureReason());
        assertTrue(result.getFailureDetail().contains(TestSagaFactory.STEP_B));
        assertEquals(1, context.compensateCount(TestSagaFactory.STEP_A));
        assertEquals(0, context.compensateCount(TestSagaFactory.STEP_B));
    }
}
