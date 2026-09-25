package com.github.h3yow.sagacoordinatorjava.tests.saga;

import com.github.h3yow.sagacoordinatorjava.saga.model.SagaContext;
import com.github.h3yow.sagacoordinatorjava.saga.model.StepOutcome;

/**
 * 正向提交失败、但补偿入口可挂起的测试参与者。
 * 用于让事务在“第一个补偿动作”执行中崩溃。
 */
public class FailExecuteGateCompensateStep extends GateCompensatingStep {

    public FailExecuteGateCompensateStep(String name) {
        super(name);
    }

    @Override
    public StepOutcome execute(SagaContext context) {
        return StepOutcome.fail(name() + " 模拟业务失败");
    }

    @Override
    public StepOutcome compensate(SagaContext context) {
        if (!isArmed()) {
            return super.compensate(context);
        }
        compensateCalls.incrementAndGet();
        compensationEntered.countDown();
        try {
            release.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return StepOutcome.fail("模拟崩溃中断");
        }
        return super.compensate(context);
    }

    private boolean isArmed() {
        return armed;
    }
}
