package com.github.h3yow.sagacoordinatorjava.tests.saga;

import com.github.h3yow.sagacoordinatorjava.saga.engine.SagaCoordinator;
import com.github.h3yow.sagacoordinatorjava.saga.engine.StepRegistry;
import com.github.h3yow.sagacoordinatorjava.saga.model.FailureReason;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaDefinition;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaResult;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaState;
import com.github.h3yow.sagacoordinatorjava.saga.store.FileEventLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景一：正常完成；场景二：参与者失败后逆序补偿；补偿失败可区分。
 */
class SagaCompletionAndCompensationTest {

    @TempDir
    Path tempDir;

    private SagaCoordinator coordinator;
    private StepRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new StepRegistry();
        coordinator = new SagaCoordinator(
                new FileEventLog(tempDir.resolve("events").toString()), registry,
                Duration.ofMinutes(10), Duration.ofMinutes(10));
    }

    @AfterEach
    void tearDown() {
        coordinator.shutdown();
    }

    private SagaDefinition def(RecordingStep... steps) {
        return SagaDefinition.builder("order")
                .steps(List.of(steps))
                .stepTimeout(Duration.ofSeconds(2))
                .sagaTimeout(Duration.ofSeconds(30))
                .compensationRetries(1)
                .build();
    }

    @Test
    void allStepsComplete() {
        RecordingStep a = new RecordingStep("inventory");
        RecordingStep b = new RecordingStep("payment");
        RecordingStep c = new RecordingStep("shipping");

        SagaResult result = coordinator.submit("saga-happy", def(a, b, c));

        System.out.printf("[轨迹][%s] 最终状态=%s 原因=%s 详情=%s%n",
                result.sagaId(), result.state(), result.reason(), result.reasonDetail());

        assertEquals(SagaState.COMPLETED, result.state());
        assertEquals(FailureReason.NONE, result.reason());
        assertEquals(1, a.executeCalls.get());
        assertEquals(1, b.executeCalls.get());
        assertEquals(1, c.executeCalls.get());
        assertEquals(0, a.compensateCalls.get());
        assertTrue(a.hasEffectFor("saga-happy"));
    }

    @Test
    void failureTriggersReverseCompensation() {
        RecordingStep a = new RecordingStep("inventory");
        RecordingStep b = new RecordingStep("payment");
        RecordingStep c = new RecordingStep("shipping", true, false);

        SagaResult result = coordinator.submit("saga-fail", def(a, b, c));

        System.out.printf("[轨迹][%s] 最终状态=%s 原因=%s 详情=%s%n",
                result.sagaId(), result.state(), result.reason(), result.reasonDetail());
        System.out.printf("[补偿计数] inventory=%d payment=%d shipping=%d%n",
                a.compensateCalls.get(), b.compensateCalls.get(), c.compensateCalls.get());

        assertEquals(SagaState.COMPENSATED, result.state());
        assertEquals(FailureReason.NONE, result.reason());
        // 逆序：payment 先于 inventory 被补偿；失败的 shipping 也尝试一次补偿
        assertEquals(1, c.compensateCalls.get());
        assertEquals(1, b.compensateCalls.get());
        assertEquals(1, a.compensateCalls.get());
        assertFalse(a.hasEffectFor("saga-fail"));
        assertFalse(b.hasEffectFor("saga-fail"));
        // 再次查询得到同一结论
        SagaResult again = coordinator.getStatus("saga-fail");
        assertEquals(SagaState.COMPENSATED, again.state());
    }

    @Test
    void compensationFailureIsExplicitlyMarked() {
        RecordingStep a = new RecordingStep("inventory", false, true);
        RecordingStep b = new RecordingStep("payment", true, false);

        SagaResult result = coordinator.submit("saga-cf", def(a, b));

        System.out.printf("[轨迹][%s] 最终状态=%s 原因=%s 详情=%s%n",
                result.sagaId(), result.state(), result.reason(), result.reasonDetail());

        assertEquals(SagaState.COMPENSATION_FAILED, result.state());
        assertEquals(FailureReason.COMPENSATION_FAILED, result.reason());
        assertTrue(result.reasonDetail().contains("inventory"));
        // 首次 + 1 次重试
        assertEquals(2, a.compensateCalls.get());
    }
}
