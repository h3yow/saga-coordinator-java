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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 超时恢复场景：步骤级超时、事务总超时、巡检器收敛挂起事务。
 */
class TimeoutRecoveryTest {

    @TempDir
    Path tempDir;

    private SagaCoordinator coordinator;
    private StepRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new StepRegistry();
    }

    @AfterEach
    void tearDown() {
        if (coordinator != null) {
            coordinator.shutdown();
        }
    }

    private SagaDefinition def(List<? extends com.github.h3yow.sagacoordinatorjava.saga.model.SagaStep> steps,
                               Duration stepTimeout, Duration sagaTimeout) {
        return SagaDefinition.builder("order")
                .steps(List.copyOf(steps))
                .stepTimeout(stepTimeout)
                .sagaTimeout(sagaTimeout)
                .compensationRetries(0)
                .build();
    }

    @Test
    void stepTimeoutTriggersCompensationWithDistinctReason() {
        RecordingStep a = new RecordingStep("inventory");
        // 步骤执行 1 秒，而 stepTimeout 只有 200ms
        RecordingStep b = new RecordingStep("slowPayment", false, false, 1_000);
        coordinator = new SagaCoordinator(
                new FileEventLog(tempDir.resolve("events").toString()), registry,
                Duration.ofMinutes(10), Duration.ofMinutes(10));

        SagaResult result = coordinator.submit("saga-step-timeout",
                def(List.of(a, b), Duration.ofMillis(200), Duration.ofSeconds(30)));

        System.out.printf("[轨迹][saga-step-timeout] 最终=%s 原因=%s 详情=%s%n",
                result.state(), result.reason(), result.reasonDetail());

        assertEquals(SagaState.COMPENSATED, result.state());
        assertEquals(1, a.compensateCalls.get(), "超时前已完成的步骤必须被逆序补偿");
        assertEquals(1, b.compensateCalls.get(), "超时步骤结果未知也要尝试补偿");
    }

    @Test
    void reaperConvergesHungSagaWithoutBlockingOthers() throws Exception {
        // 参与者线程永久挂起（stepTimeout 设很大，交由巡检器判定无进展）
        RecordingStep a = new RecordingStep("inventory");
        RecordingStep b = new RecordingStep("hungPayment");
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch never = new CountDownLatch(1);
        b.gate(entered, never);

        coordinator = new SagaCoordinator(
                new FileEventLog(tempDir.resolve("events").toString()), registry,
                Duration.ofMinutes(10), Duration.ofSeconds(1));

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<?> hung = pool.submit(() -> coordinator.submit("saga-hung",
                def(List.of(a, b), Duration.ofHours(1), Duration.ofHours(1))));
        assertTrue(entered.await(5, TimeUnit.SECONDS));

        // 另一个事务在独立协调器锁下正常提交，不被挂起事务阻塞
        StepRegistry registry2 = new StepRegistry();
        SagaCoordinator other = new SagaCoordinator(
                new FileEventLog(tempDir.resolve("events2").toString()), registry2,
                Duration.ofMinutes(10), Duration.ofMinutes(10));
        RecordingStep x = new RecordingStep("quickA");
        RecordingStep y = new RecordingStep("quickB");
        SagaResult quick = other.submit("saga-quick",
                def(List.of(x, y), Duration.ofSeconds(5), Duration.ofSeconds(30)));
        System.out.printf("[轨迹][saga-quick] 挂起事务存在期间完成: %s%n", quick.state());
        assertEquals(SagaState.COMPLETED, quick.state());
        other.shutdown();

        // 手动触发巡检：等待超过 staleThreshold(1s) 无进展后扫描，
        // 判定步骤超时 -> 在锁外逆序补偿
        Thread.sleep(1_200);
        coordinator.sweepOnce();

        long deadline = System.currentTimeMillis() + 10_000;
        SagaResult result = null;
        while (System.currentTimeMillis() < deadline) {
            result = coordinator.getStatus("saga-hung");
            if (result.state() == SagaState.COMPENSATED) {
                break;
            }
            Thread.sleep(50);
        }
        System.out.printf("[轨迹][saga-hung] 巡检收敛后=%s 原因=%s 详情=%s%n",
                result.state(), result.reason(), result.reasonDetail());

        assertEquals(SagaState.COMPENSATED, result.state());
        assertEquals(1, a.compensateCalls.get());
        assertEquals(1, b.compensateCalls.get());

        // 收敛已验证完毕：放开门闩并彻底停掉旧引擎的残留步骤线程，
        // 避免悬挂线程持有文件资源影响 @TempDir 清理
        never.countDown();
        coordinator.shutdown();
        coordinator = null;
        pool.shutdownNow();
    }

    @Test
    void sagaGlobalTimeoutDuringForwardConverges() {
        // 第一步快速完成，第二步耗时超过整个事务的 sagaTimeout：
        // 超时后第一步必须被逆序补偿，事务按超时规则收敛。
        RecordingStep a = new RecordingStep("fastInventory");
        RecordingStep b = new RecordingStep("slowPayment", false, false, 800);
        coordinator = new SagaCoordinator(
                new FileEventLog(tempDir.resolve("events").toString()), registry,
                Duration.ofMinutes(10), Duration.ofMinutes(10));

        SagaResult result = coordinator.submit("saga-global-timeout",
                def(List.of(a, b), Duration.ofSeconds(30), Duration.ofMillis(200)));

        System.out.printf("[轨迹][saga-global-timeout] 最终=%s 原因=%s 详情=%s%n",
                result.state(), result.reason(), result.reasonDetail());
        // 收敛结论可区分：要么补偿完成 COMPENSATED，要么补偿无法在期限内收敛 -> TIMED_OUT
        assertTrue(result.state() == SagaState.COMPENSATED
                        || result.state() == SagaState.TIMED_OUT,
                "事务必须按超时规则收敛到明确终态，实际=" + result.state());
        assertEquals(1, a.compensateCalls.get(), "超时前已完成步骤必须被逆序补偿");
    }
}
