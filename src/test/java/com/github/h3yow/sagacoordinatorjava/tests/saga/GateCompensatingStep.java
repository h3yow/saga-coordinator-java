package com.github.h3yow.sagacoordinatorjava.tests.saga;

import com.github.h3yow.sagacoordinatorjava.saga.model.SagaContext;
import com.github.h3yow.sagacoordinatorjava.saga.model.StepOutcome;

import java.util.concurrent.CountDownLatch;

/**
 * 补偿入口可被门闩挂起的测试参与者（模拟补偿执行中进程崩溃）。
 * 显式调用 {@link #arm()} 后门闩才生效；重启后的新实例默认不挂起。
 */
public class GateCompensatingStep extends RecordingStep {

    final CountDownLatch compensationEntered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    protected volatile boolean armed;

    public GateCompensatingStep(String name) {
        super(name);
    }

    public void arm() {
        this.armed = true;
    }

    @Override
    public StepOutcome compensate(SagaContext context) {
        if (!armed) {
            return super.compensate(context);
        }
        compensateCalls.incrementAndGet();
        compensationEntered.countDown();
        try {
            release.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // 被 shutdown 中断 = 模拟进程在此刻崩溃
            return StepOutcome.fail("模拟崩溃中断");
        }
        return super.compensate(context);
    }
}
