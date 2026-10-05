package com.github.h3yow.sagacoordinatorjava.store;

import com.github.h3yow.sagacoordinatorjava.model.SagaTransaction;
import com.github.h3yow.sagacoordinatorjava.model.TransactionSummary;

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
     * 流式扫描全部事务摘要：逐个回调，不把完整事务（含可能很长的事件轨迹）
     * 一次性读入内存。供总览筛选 / 分页查询使用；只读，不与事务推进争锁。
     */
    void scanSummaries(Consumer<TransactionSummary> consumer);
}
