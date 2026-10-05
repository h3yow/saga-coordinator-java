package com.github.h3yow.sagacoordinatorjava.coordinator;

/**
 * 人工处置请求被拒绝：事务当前状态不允许该处置动作
 * （例如对已结束事务发起重试、对运行中事务标记人工处理完毕）。
 *
 * <p>属于调用方可纠正的请求错误，不改变任何事务状态，也不产生业务效果。
 */
public class ManualActionRejectedException extends RuntimeException {

    public ManualActionRejectedException(String message) {
        super(message);
    }
}
