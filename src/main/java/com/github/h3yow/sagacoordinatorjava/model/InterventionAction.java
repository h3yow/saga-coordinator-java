package com.github.h3yow.sagacoordinatorjava.model;

/**
 * 人工介入处置动作。
 */
public enum InterventionAction {
    /** 对补偿未收敛的事务再发起一次补偿收尾 */
    RETRY_COMPENSATION,
    /** 显式标记该事务已由人工处理完毕 */
    MARK_RESOLVED
}
