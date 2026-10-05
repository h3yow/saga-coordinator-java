package com.github.h3yow.sagacoordinatorjava.store;

import com.github.h3yow.sagacoordinatorjava.model.SagaTransaction;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * 事务状态持久化抽象。每个事务以独立文件保存，写入采用原子替换。
 */
public interface TransactionStore {

    /**
     * 若不存在则创建，已存在则返回已有事务（幂等提交的基础）。
     */
    SagaTransaction createIfAbsent(SagaTransaction transaction);

    /**
     * 原子保存事务最新状态。
     */
    void save(SagaTransaction transaction);

    Optional<SagaTransaction> find(String transactionId);

    /**
     * 扫描所有事务（崩溃恢复 / 超时巡检使用）。
     */
    List<SagaTransaction> findAll();

    /**
     * 逐条流式访问所有事务（总览查询使用）。
     * 实现必须保证任意时刻只在内存中保留当前一条，
     * 事务总量上千时查询内存占用不随之线性增长。
     */
    void forEach(Consumer<SagaTransaction> consumer);
}
