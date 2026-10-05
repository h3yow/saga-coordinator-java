package com.github.h3yow.sagacoordinatorjava.tests.support;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 测试用参与者上下文：记录每个参与者步骤的正向调用、补偿调用次数，
 * 支持注入失败与补偿失败，并以并发集合支持并发提交测试。
 */
public class TestContext {

    public final ConcurrentMap<String, Integer> executeCalls = new ConcurrentHashMap<>();
    public final ConcurrentMap<String, Integer> compensateCalls = new ConcurrentHashMap<>();
    public final List<String> executedSteps = java.util.Collections.synchronizedList(new ArrayList<>());
    public final List<String> compensatedSteps = java.util.Collections.synchronizedList(new ArrayList<>());

    public volatile String failStep = null;
    public volatile String compensationFailStep = null;
    public volatile String slowStep = null;
    public volatile long slowMillis = 0;
    /** 设置后，该步骤的正向调用进入后在 gate 上等待（可被释放 / 中断），并在进入时放行 entered。 */
    public volatile String hangStep = null;
    public final java.util.concurrent.CountDownLatch hangEntered =
            new java.util.concurrent.CountDownLatch(1);
    public volatile java.util.concurrent.CountDownLatch hangGate =
            new java.util.concurrent.CountDownLatch(1);

    public void execute(String step) {
        executeCalls.merge(step, 1, Integer::sum);
        executedSteps.add(step);
        if (hangStep != null && hangStep.equals(step)) {
            hangEntered.countDown();
            try {
                // 默认永久等待；测试通过 countDown hangGate 或 shutdownNow 中断来放行
                hangGate.await(60, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (slowStep != null && slowStep.equals(step) && slowMillis > 0) {
            try {
                Thread.sleep(slowMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (failStep != null && failStep.equals(step)) {
            throw new RuntimeException(step + " business failure injected");
        }
    }

    public void compensate(String step) {
        compensateCalls.merge(step, 1, Integer::sum);
        compensatedSteps.add(step);
        if (compensationFailStep != null && compensationFailStep.equals(step)) {
            throw new RuntimeException(step + " compensation failure injected");
        }
    }

    public int executeCount(String step) {
        return executeCalls.getOrDefault(step, 0);
    }

    public int compensateCount(String step) {
        return compensateCalls.getOrDefault(step, 0);
    }
}
