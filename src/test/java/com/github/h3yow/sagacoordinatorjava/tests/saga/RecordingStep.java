package com.github.h3yow.sagacoordinatorjava.tests.saga;

import com.github.h3yow.sagacoordinatorjava.saga.model.SagaContext;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaStep;
import com.github.h3yow.sagacoordinatorjava.saga.model.StepOutcome;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 可编排行为的测试参与者：
 * 计数 execute/compensate 调用次数；execute 与 compensate 均以
 * sagaId+步骤名做幂等（重复调用只产生一次有效效果）。
 */
public class RecordingStep implements SagaStep {

    private final String name;
    private final boolean failExecute;
    private final boolean failCompensate;
    private final long executeDelayMillis;

    final AtomicInteger executeCalls = new AtomicInteger();
    final AtomicInteger compensateCalls = new AtomicInteger();
    final ConcurrentHashMap<String, String> executed = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, String> compensated = new ConcurrentHashMap<>();

    volatile CountDownLatch executeEntered;
    volatile CountDownLatch releaseExecute;

    public RecordingStep(String name) {
        this(name, false, false, 0);
    }

    public RecordingStep(String name, boolean failExecute, boolean failCompensate) {
        this(name, failExecute, failCompensate, 0);
    }

    public RecordingStep(String name, boolean failExecute, boolean failCompensate,
                         long executeDelayMillis) {
        this.name = name;
        this.failExecute = failExecute;
        this.failCompensate = failCompensate;
        this.executeDelayMillis = executeDelayMillis;
    }

    /** 在 execute 入口等待放行（用于模拟崩溃时步骤仍在运行）。 */
    public void gate(CountDownLatch entered, CountDownLatch release) {
        this.executeEntered = entered;
        this.releaseExecute = release;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public StepOutcome execute(SagaContext context) {
        executeCalls.incrementAndGet();
        if (executeEntered != null) {
            executeEntered.countDown();
        }
        if (releaseExecute != null) {
            try {
                releaseExecute.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (executeDelayMillis > 0 && !failExecute) {
            try {
                Thread.sleep(executeDelayMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        String key = context.getSagaId() + "#" + name;
        if (executed.putIfAbsent(key, "done") != null) {
            return StepOutcome.ok("幂等命中，无重复效果");
        }
        if (failExecute) {
            executed.remove(key);
            return StepOutcome.fail(name + " 模拟业务失败");
        }
        context.put("step." + name, "executed");
        return StepOutcome.ok(name + " 已提交");
    }

    @Override
    public StepOutcome compensate(SagaContext context) {
        compensateCalls.incrementAndGet();
        String key = context.getSagaId() + "#" + name;
        if (!executed.containsKey(key)) {
            // 正向未生效：补偿幂等为空操作，不报错
            return StepOutcome.ok(name + " 无需补偿");
        }
        if (compensated.putIfAbsent(key, "undone") != null) {
            return StepOutcome.ok("补偿幂等命中");
        }
        if (failCompensate) {
            compensated.remove(key);
            return StepOutcome.fail(name + " 模拟补偿失败");
        }
        return StepOutcome.ok(name + " 已补偿");
    }

    public boolean hasEffectFor(String sagaId) {
        return executed.containsKey(sagaId + "#" + name)
                && !compensated.containsKey(sagaId + "#" + name);
    }
}
