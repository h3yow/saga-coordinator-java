package com.github.h3yow.sagacoordinatorjava.tests.saga;

import com.github.h3yow.sagacoordinatorjava.saga.engine.SagaCoordinator;
import com.github.h3yow.sagacoordinatorjava.saga.engine.StepRegistry;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景三：幂等与并发控制。
 */
class DuplicateAndConcurrentSubmitTest {

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
                .stepTimeout(Duration.ofSeconds(5))
                .sagaTimeout(Duration.ofSeconds(60))
                .compensationRetries(0)
                .build();
    }

    @Test
    void duplicateSubmissionProducesOneEffectAndSameConclusion() {
        RecordingStep a = new RecordingStep("inventory");
        RecordingStep b = new RecordingStep("payment");
        SagaDefinition definition = def(a, b);

        SagaResult first = coordinator.submit("saga-dup", definition);
        SagaResult second = coordinator.submit("saga-dup", definition);
        SagaResult third = coordinator.submit("saga-dup", definition);

        System.out.printf("[轨迹][saga-dup] 三次重复提交结论: %s / %s / %s%n",
                first.state(), second.state(), third.state());

        assertEquals(SagaState.COMPLETED, first.state());
        assertEquals(first.state(), second.state());
        assertEquals(first.state(), third.state());
        assertEquals(1, a.executeCalls.get(), "重复提交不得产生第二次业务效果");
        assertEquals(1, b.executeCalls.get());
    }

    @Test
    void concurrentSubmissionsAreSerializedToSingleEffect() throws Exception {
        // 用门闩让第一个线程停在第二步执行中，其它线程并发提交同一事务
        RecordingStep a = new RecordingStep("inventory");
        RecordingStep b = new RecordingStep("payment");
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        b.gate(entered, release);

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<SagaResult> results = new ConcurrentLinkedQueue<>();
        Set<SagaState> observedRunning = new HashSet<>();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                SagaResult r = coordinator.submit("saga-concurrent", def(a, b));
                synchronized (observedRunning) {
                    observedRunning.add(r.state());
                }
                results.add(r);
            });
        }
        start.countDown();
        assertTrue(entered.await(5, TimeUnit.SECONDS), "首个提交应进入第二步");

        // 参与者在锁外执行期间，查询可以看到一致的中间态 RUNNING（但不会是
        // 撕裂的非法状态）；关键是所有并发“提交”都必须阻塞到终结、拿到同一结论。
        SagaResult midView = coordinator.getStatus("saga-concurrent");
        Future<SagaResult> concurrentSubmitter = pool.submit(
                () -> coordinator.submit("saga-concurrent", def(a, b)));
        Thread.sleep(100);
        release.countDown();

        SagaResult during = concurrentSubmitter.get(10, TimeUnit.SECONDS);

        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        System.out.printf("[轨迹][saga-concurrent] 并发返回数=%d, 提交结论集合=%s, 执行中查询=%s%n",
                results.size(), observedRunning,
                midView == null ? "不可见" : midView.state());

        assertEquals(SagaState.RUNNING, midView.state(), "锁外执行期间允许看到一致中间态");
        assertEquals(SagaState.COMPLETED, during.state(),
                "并发提交不得返回中间态，只能拿到终结结论");

        assertEquals(threads, results.size());
        for (SagaResult r : results) {
            assertEquals(SagaState.COMPLETED, r.state(), "所有并发提交拿到同一终结结论");
        }
        assertEquals(1, a.executeCalls.get(), "并发提交只产生一次业务效果");
        assertEquals(1, b.executeCalls.get());
    }

    @Test
    void differentSagasDoNotBlockEachOther() throws Exception {
        RecordingStep slow = new RecordingStep("slowStep", false, false, 800);
        RecordingStep fast = new RecordingStep("fastStep");

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        Future<?> f1 = pool.submit(() -> {
            try {
                start.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            coordinator.submit("saga-A", def(slow));
        });
        Future<SagaResult> f2 = pool.submit(() -> {
            try {
                start.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return coordinator.submit("saga-B", def(fast));
        });
        start.countDown();
        SagaResult resultB = f2.get(5, TimeUnit.SECONDS);
        f1.get(10, TimeUnit.SECONDS);

        System.out.printf("[轨迹] saga-B 在 saga-A 慢速期间完成: %s%n", resultB.state());
        assertEquals(SagaState.COMPLETED, resultB.state());
    }
}
