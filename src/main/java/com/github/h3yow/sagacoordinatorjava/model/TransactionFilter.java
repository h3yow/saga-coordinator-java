package com.github.h3yow.sagacoordinatorjava.model;

/**
 * 事务总览查询条件：可按事务状态、业务类型（sagaType）筛选并分页。
 * 任一字段为 null 表示该维度不过滤。页号从 0 开始。
 */
public class TransactionFilter {

    /** 单页最大条数，防止一次性拉取过多。 */
    public static final int MAX_PAGE_SIZE = 200;

    private TransactionStatus status;
    private String sagaType;
    private int page;
    private int size;

    public TransactionFilter() {
    }

    public TransactionFilter(TransactionStatus status, String sagaType, int page, int size) {
        this.status = status;
        this.sagaType = sagaType;
        this.page = page;
        this.size = size;
    }

    public TransactionStatus getStatus() {
        return status;
    }

    public void setStatus(TransactionStatus status) {
        this.status = status;
    }

    public String getSagaType() {
        return sagaType;
    }

    public void setSagaType(String sagaType) {
        this.sagaType = sagaType;
    }

    public int getPage() {
        return page;
    }

    public void setPage(int page) {
        this.page = page;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
