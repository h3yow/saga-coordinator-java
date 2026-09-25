package com.github.h3yow.sagacoordinatorjava.tests.saga;

import com.github.h3yow.sagacoordinatorjava.saga.engine.SagaCoordinator;
import com.github.h3yow.sagacoordinatorjava.saga.engine.StepRegistry;
import com.github.h3yow.sagacoordinatorjava.saga.model.FailureReason;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaDefinition;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaStep;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaResult;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaState;
import com.github.h3yow.sagacoordinatorjava.saga.store.FileEventLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景四：状态持久化与崩溃恢复。
 * “重启”= 丢弃旧协调器（含内存态/线程池），基于同一事件日志目录新建协调器。
 */
class RestartRecoveryTest {

    @TempDir
    Path tempDir;

    private SagaCoordinator coordinator;
    private SagaCoordinator crashedCoordinator;

    /**
     * 崩溃模拟期间留在门闩上的步骤线程：断言完成后再真正中断，
     * 避免残留线程影响 @TempDir 清理（模拟语义已在“中断前”验证完毕）。
     */
    private void cleanUpCrashedCoordinator() {
        if (crashedCoordinator != null) {
            crashedCoordinator.shutdown();
            crashedCoordinator = null;
        }
    }

    private SagaCoordinator newCoordinator(StepRegistry registry,
                                           Duration staleThreshold) {
        return new SagaCoordinator(
                new FileEventLog(tempDir.resolve("events").toString()), registry,
                Duration.ofMinutes(10), staleThreshold);
    }

    private SagaDefinition def(List<? extends SagaStep> steps,
                               Duration stepTimeout, Duration sagaTimeout, int retries) {
        return SagaDefinition.builder("order")
                .steps(List.copyOf(steps))
                .stepTimeout(stepTimeout)
                .sagaTimeout(sagaTimeout)
                .compensationRetries(retries)
                .build();
    }

    private SagaResult awaitTerminal(SagaCoordinator c, String sagaId, long millis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + millis;
        while (System.currentTimeMillis() < deadline) {
            SagaResult r = c.getStatus(sagaId);
            if (r != null && isTerminal(r.state())) {
                return r;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("事务在 " + millis + "ms 内未终结: "
                + (c.getStatus(sagaId)));
    }

    private boolean isTerminal(SagaState state) {
        return switch (state) {
            case COMPLETED, COMPENSATED, COMPENSATION_FAILED, FAILED,
                 TIMED_OUT, MANUAL_INTERVENTION -> true;
            default -> false;
        };
    }

    @AfterEach
    void tearDown() {
        if (coordinator != null) {
            coordinator.shutdown();
        }
    }

    @Test
    void completedSagaSurvivesRestartWithSameConclusion() {
        StepRegistry registry = new StepRegistry();
        coordinator = newCoordinator(registry, Duration.ofMinutes(10));
        RecordingStep a = new RecordingStep("inventory");
        RecordingStep b = new RecordingStep("payment");
        registry.register(a);
        registry.register(b);

        SagaResult before = coordinator.submit("saga-persist-ok",
                def(List.of(a, b), Duration.ofSeconds(2), Duration.ofSeconds(30), 0));
        assertEquals(SagaState.COMPLETED, before.state());

        // 重启：全新注册表与协调器，仅靠事件日志还原
        coordinator.shutdown();
        StepRegistry registry2 = new StepRegistry();
        registry2.register(new RecordingStep("inventory"));
        registry2.register(new RecordingStep("payment"));
        SagaCoordinator restarted = newCoordinator(registry2, Duration.ofMinutes(10));
        try {
            restarted.recoverAfterRestart();
            SagaResult after = restarted.getStatus("saga-persist-ok");
            System.out.printf("[轨迹][saga-persist-ok] 重启前=%s 重启后=%s%n",
                    before.state(), after == null ? "null" : after.state());
            assertNotNull(after);
            assertEquals(SagaState.COMPLETED, after.state());
        } finally {
            restarted.shutdown();
        }
    }

    @Test
    void crashDuringForwardStepResumesAndConverges() throws Exception {
        StepRegistry registry = new StepRegistry();
        RecordingStep a = new RecordingStep("inventory");
        RecordingStep b = new RecordingStep("payment");
        // 第三个步骤模拟“执行中进程被杀”：线程永远卡在门闩上
        RecordingStep c = new RecordingStep("shipping");
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch neverRelease = new CountDownLatch(1);
        c.gate(entered, neverRelease);
        registry.register(a);
        registry.register(b);
        registry.register(c);
        coordinator = newCoordinator(registry, Duration.ofSeconds(1));

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<?> f = pool.submit(() -> coordinator.submit("saga-crash-fwd",
                def(List.of(a, b, c), Duration.ofHours(1), Duration.ofHours(1), 0)));
        assertTrue(entered.await(5, TimeUnit.SECONDS));

        // “崩溃”：只抛弃协调器，卡住的步骤线程停在原地，引擎来不及写后续事件
        crashedCoordinator = coordinator;
        coordinator = null;
        crashedCoordinator.detach();
        pool.shutdown();

        // 重启：新协调器 + 新参与者实例；挂起步骤会被超时巡检判定为无进展
        StepRegistry registry2 = new StepRegistry();
        RecordingStep a2 = new RecordingStep("inventory");
        RecordingStep b2 = new RecordingStep("payment");
        RecordingStep c2 = new RecordingStep("shipping", true, false);
        registry2.register(a2);
        registry2.register(b2);
        registry2.register(c2);
        SagaCoordinator restarted = newCoordinator(registry2, Duration.ofSeconds(1));
        try {
            restarted.recoverAfterRestart();
            SagaResult result = awaitTerminal(restarted, "saga-crash-fwd", 15_000);

            System.out.printf("[轨迹][saga-crash-fwd] 恢复后最终=%s 原因=%s 详情=%s%n",
                    result.state(), result.reason(), result.reasonDetail());
            System.out.printf("[补偿计数-新进程] inventory=%d payment=%d shipping=%d%n",
                    a2.compensateCalls.get(), b2.compensateCalls.get(),
                    c2.compensateCalls.get());

            assertEquals(SagaState.COMPENSATED, result.state());
            assertEquals(FailureReason.NONE, result.reason());
            assertEquals(1, a2.compensateCalls.get(), "已完成步骤必须逆序补偿");
            assertEquals(1, b2.compensateCalls.get());
            assertEquals(1, c2.compensateCalls.get(), "结果未知的挂起步骤也要尝试补偿");
        } finally {
            restarted.shutdown();
            cleanUpCrashedCoordinator();
        }
    }

    @Test
    void crashDuringCompensationResumesFullReverseOrder() throws Exception {
        // 在“第一个被补偿的步骤”（失败的 shipping）补偿执行中模拟崩溃，
        // 此时 payment/inventory 尚未补偿，验证重启后补偿锚点仍覆盖完整逆序。
        StepRegistry registry = new StepRegistry();
        RecordingStep a = new RecordingStep("inventory");
        RecordingStep b = new RecordingStep("payment");
        FailExecuteGateCompensateStep c = new FailExecuteGateCompensateStep("shipping");
        c.arm();
        registry.register(a);
        registry.register(b);
        registry.register(c);
        coordinator = newCoordinator(registry, Duration.ofMinutes(10));

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<?> f = pool.submit(() -> coordinator.submit("saga-crash-comp",
                def(List.of(a, b, c), Duration.ofSeconds(2), Duration.ofMinutes(10), 0)));
        assertTrue(c.compensationEntered.await(5, TimeUnit.SECONDS),
                "应已进入 shipping 的补偿");

        crashedCoordinator = coordinator;
        coordinator = null;
        crashedCoordinator.detach();
        // 模拟进程瞬间消失：挂起的补偿线程永不放行（真实崩溃中线程直接死亡，
        // 旧引擎没有任何机会再写事件）。该守护线程在断言后由 cleanup 中断。
        pool.shutdown();

        StepRegistry registry2 = new StepRegistry();
        RecordingStep a2 = new RecordingStep("inventory");
        RecordingStep b2 = new RecordingStep("payment");
        FailExecuteGateCompensateStep c2 =
                new FailExecuteGateCompensateStep("shipping"); // 重启后不再挂起
        registry2.register(a2);
        registry2.register(b2);
        registry2.register(c2);
        SagaCoordinator restarted = newCoordinator(registry2, Duration.ofMinutes(10));
        try {
            restarted.recoverAfterRestart();
            SagaResult result = awaitTerminal(restarted, "saga-crash-comp", 10_000);

            System.out.printf("[轨迹][saga-crash-comp] 恢复后最终=%s 原因=%s%n",
                    result.state(), result.reason());
            System.out.printf("[补偿计数-新进程] inventory=%d payment=%d shipping=%d%n",
                    a2.compensateCalls.get(), b2.compensateCalls.get(),
                    c2.compensateCalls.get());

            assertEquals(SagaState.COMPENSATED, result.state());
            assertEquals(1, c2.compensateCalls.get());
            assertEquals(1, b2.compensateCalls.get());
            assertEquals(1, a2.compensateCalls.get(),
                    "补偿锚点之后的步骤在重启后仍须全部补偿");
        } finally {
            restarted.shutdown();
            cleanUpCrashedCoordinator();
        }
    }
}
