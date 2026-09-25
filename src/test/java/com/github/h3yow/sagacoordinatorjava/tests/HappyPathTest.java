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
 * 场景一：所有参与者步骤正常完成。
 */
class HappyPathTest {

    private static final Logger log = LoggerFactory.getLogger(HappyPathTest.class);

    @TempDir
    Path tempDir;

    @Test
    void allStepsCompleteInOrder() {
        SagaCoordinator coordinator =
                TestSagaFactory.newCoordinator(tempDir, 0, 60_000, 3);
        TestContext context = new TestContext();
        String txId = "tx-happy-001";

        TransactionResult result = coordinator.submit(
                TestSagaFactory.threeStepDefinition(), txId, context);

        log.info("tx={} finalStatus={} reason={}",
                result.getTransactionId(), result.getStatus(), result.getFailureReason());
        result.getTrace().forEach(e -> log.info("tx={} trace {}", txId, e));

        assertEquals(TransactionStatus.COMPLETED, result.getStatus());
        assertEquals(FailureReason.NONE, result.getFailureReason());
        assertEquals(1, context.executeCount(TestSagaFactory.STEP_A));
        assertEquals(1, context.executeCount(TestSagaFactory.STEP_B));
        assertEquals(1, context.executeCount(TestSagaFactory.STEP_C));
        assertEquals(0, context.compensateCount(TestSagaFactory.STEP_A));
        assertEquals(List.of(TestSagaFactory.STEP_A, TestSagaFactory.STEP_B, TestSagaFactory.STEP_C),
                context.executedSteps);

        // 轨迹中必须能看到完整的状态迁移
        assertTrue(result.getTrace().stream().anyMatch(e -> e.getDetail() != null
                && e.getDetail().contains("RUNNING -> COMPLETED")));
    }
}
