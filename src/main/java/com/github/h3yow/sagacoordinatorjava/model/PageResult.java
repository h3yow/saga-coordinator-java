package com.github.h3yow.sagacoordinatorjava.model;

import java.util.List;

/**
 * 分页结果包装。
 *
 * @param <T> 列表元素类型
 */
public class PageResult<T> {

    private List<T> items;
    /** 从 0 开始的页号。 */
    private int page;
    private int size;
    private long totalItems;
    private int totalPages;

    public PageResult() {
    }

    public PageResult(List<T> items, int page, int size, long totalItems) {
        this.items = items;
        this.page = page;
        this.size = size;
        this.totalItems = totalItems;
        this.totalPages = size <= 0 ? 0 : (int) ((totalItems + size - 1) / size);
    }

    public List<T> getItems() {
        return items;
    }

    public void setItems(List<T> items) {
        this.items = items;
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

    public long getTotalItems() {
        return totalItems;
    }

    public void setTotalItems(long totalItems) {
        this.totalItems = totalItems;
    }

    public int getTotalPages() {
        return totalPages;
    }

    public void setTotalPages(int totalPages) {
        this.totalPages = totalPages;
    }
}
