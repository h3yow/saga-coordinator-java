package com.github.h3yow.sagacoordinatorjava.saga.store;

import java.util.List;

/**
 * 每个 sagaId 一个追加式事件日志。
 */
public interface EventLog {

    /** 追加一条事件并返回其全局序号；写盘成功后返回。 */
    SagaEvent append(SagaEvent event);

    /** 读取某事务的全部事件（按序号升序）。 */
    List<SagaEvent> read(String sagaId);

    /** 当前已持久化的所有事务标识。 */
    List<String> sagaIds();
}
