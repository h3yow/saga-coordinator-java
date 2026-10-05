package com.github.h3yow.sagacoordinatorjava.coordinator;

import com.github.h3yow.sagacoordinatorjava.model.PageResult;
import com.github.h3yow.sagacoordinatorjava.model.TransactionStatus;
import com.github.h3yow.sagacoordinatorjava.model.TransactionSummary;
import com.github.h3yow.sagacoordinatorjava.store.TransactionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 事务总览查询：按状态 / 业务类型筛选并分页返回轻量摘要。
 *
 * <p>只读取持久化快照、不触碰任何事务锁，查询不会干扰正在推进的事务。
 * 逐条流式扫描，排序只保留大小为 offset+size 的有界堆，
 * 内存占用与事务总量无关，上千条事务也能稳定分页。
 */
public class TransactionQueryService {

    private static final Logger log = LoggerFactory.getLogger(TransactionQueryService.class);

    /** 列表排序：最近有进展的排前面；时间相同按事务 ID 升序，保证分页稳定 */
    private static final Comparator<TransactionSummary> ORDER =
            Comparator.comparingLong(TransactionSummary::getUpdatedAt).reversed()
                    .thenComparing(TransactionSummary::getTransactionId);

    private final TransactionStore store;

    public TransactionQueryService(TransactionStore store) {
        this.store = store;
    }

    /**
     * @param status   按状态筛选，null 表示不限
     * @param sagaType 按业务类型筛选，null / 空白表示不限
     * @param page     页码，从 0 开始
     * @param size     每页大小，1..200
     */
    public PageResult<TransactionSummary> query(TransactionStatus status, String sagaType,
                                                int page, int size) {
        if (page < 0) {
            throw new IllegalArgumentException("page must be >= 0");
        }
        if (size < 1 || size > 200) {
            throw new IllegalArgumentException("size must be in [1, 200]");
        }
        String typeFilter = (sagaType == null || sagaType.isBlank()) ? null : sagaType;
        long offset = (long) page * size;
        int keep = (int) Math.min(offset + size, 100_000);

        // 堆顶放"最差"元素：堆满后只保留最靠前的 keep 条，内存有界
        PriorityQueue<TransactionSummary> heap = new PriorityQueue<>(ORDER.reversed());
        AtomicLong total = new AtomicLong();
        store.forEach(tx -> {
            if (status != null && tx.getStatus() != status) {
                return;
            }
            if (typeFilter != null && !typeFilter.equals(tx.getSagaType())) {
                return;
            }
            total.incrementAndGet();
            TransactionSummary summary = TransactionSummary.of(tx);
            if (heap.size() < keep) {
                heap.add(summary);
            } else if (ORDER.compare(summary, heap.peek()) < 0) {
                heap.poll();
                heap.add(summary);
            }
        });

        List<TransactionSummary> sorted = new ArrayList<>(heap);
        sorted.sort(ORDER);
        List<TransactionSummary> items = sorted.size() > offset
                ? List.copyOf(sorted.subList((int) offset, sorted.size()))
                : List.of();
        log.debug("query status={} sagaType={} page={} size={} -> total={}, returned={}",
                status, typeFilter, page, size, total.get(), items.size());
        return new PageResult<>(items, page, size, total.get());
    }
}
