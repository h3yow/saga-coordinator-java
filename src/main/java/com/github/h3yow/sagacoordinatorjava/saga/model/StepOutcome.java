package com.github.h3yow.sagacoordinatorjava.saga.model;

/**
 * 参与者步骤执行/补偿的结果。
 *
 * @param success 是否成功
 * @param message 可解释的附加信息
 */
public record StepOutcome(boolean success, String message) {

    public static StepOutcome ok() {
        return new StepOutcome(true, "OK");
    }

    public static StepOutcome ok(String message) {
        return new StepOutcome(true, message);
    }

    public static StepOutcome fail(String message) {
        return new StepOutcome(false, message);
    }
}
