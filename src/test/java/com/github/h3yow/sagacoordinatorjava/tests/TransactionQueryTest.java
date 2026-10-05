package com.github.h3yow.sagacoordinatorjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.sagacoordinatorjava.coordinator.TransactionQueryService;
import com.github.h3yow.sagacoordinatorjava.model.FailureReason;
import com.github.h3yow.sagacoordinatorjava.model.PageResult;
import com.github.h3yow.sagacoordinatorjava.model.SagaTransaction;
import com.github.h3yow.sagacoordinatorjava.model.TransactionFilter;
import com.github.h3yow.sagacoordinatorjava.model.TransactionStatus;
import com.github.h3yow.sagacoordinatorjava.model.TransactionSummary;
import com.github.h3yow.sagacoordinatorjava.store.FileTransactionStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 事务总览查询：按状态 / 业务类型筛选、按最近进展时间倒序、稳定分页，
 * 以及上千条事务规模下分页返回的摘要不携带步骤明细。
 */
class TransactionQueryTest {

    private static final Logger log = LoggerFactory.getLogger(TransactionQueryTest.class);

    @TempDir
    Path tempDir;

    private SagaTransaction persist(String txId, String sagaType, TransactionStatus status,
                                    FailureReason reason, String detail,
                                    long createdAt, long updatedAt) {
        FileTransactionStore store = new FileTransactionStore(tempDir, new ObjectMapper());
        SagaTransaction tx = new SagaTransaction();
        tx.setTransactionId(txId);
        tx.setSagaType(sagaType);
        tx.setStatus(status);
        tx.setFailureReason(reason);
        tx.setFailureDetail(detail);
        tx.setCreatedAt(createdAt);
        tx.setUpdatedAt(updatedAt);
        tx.setDeadlineAt(0);
        store.save(tx);
        return tx;
    }

    private TransactionQueryService service() {
        return new TransactionQueryService(new FileTransactionStore(tempDir, new ObjectMapper()));
    }

    @Test
    void filtersByStatusAndSagaType() {
        persist("tx-001", "order", TransactionStatus.COMPLETED, FailureReason.NONE, null, 1, 10);
        persist("tx-002", "order", TransactionStatus.COMPENSATION_FAILED,
                FailureReason.COMPENSATION_FAILED, "step B stuck", 2, 20);
        persist("tx-003", "payment", TransactionStatus.COMPLETED, FailureReason.NONE, null, 3, 30);
        persist("tx-004", "order", TransactionStatus.RUNNING, FailureReason.NONE, null, 4, 40);

        TransactionQueryService svc = service();

        PageResult<TransactionSummary> running =
                svc.query(new TransactionFilter(TransactionStatus.RUNNING, null, 0, 10));
        assertEquals(1, running.getTotalItems());
        assertEquals("tx-004", running.getItems().get(0).getTransactionId());

        PageResult<TransactionSummary> orderOnly =
                svc.query(new TransactionFilter(null, "order", 0, 10));
        assertEquals(3, orderOnly.getTotalItems());
        assertTrue(orderOnly.getItems().stream().allMatch(s -> "order".equals(s.getSagaType())));

        PageResult<TransactionSummary> orderFailed = svc.query(
                new TransactionFilter(TransactionStatus.COMPENSATION_FAILED, "order", 0, 10));
        assertEquals(1, orderFailed.getTotalItems());
        TransactionSummary stuck = orderFailed.getItems().get(0);
        assertEquals("tx-002", stuck.getTransactionId());
        assertEquals("step B stuck", stuck.getFailureDetail());
        log.info("stuck tx summary: id={} status={} reason={} detail={} updatedAt={}",
                stuck.getTransactionId(), stuck.getStatus(), stuck.getFailureReason(),
                stuck.getFailureDetail(), stuck.getUpdatedAt());
    }

    @Test
    void pagesAreSortedByLatestProgressDescAndStable() {
        for (int i = 0; i < 25; i++) {
            // updatedAt 故意相同，验证事务 ID 兜底排序保证分页稳定
            persist(String.format("tx-%03d", i), "order",
                    TransactionStatus.COMPLETED, FailureReason.NONE, null, 1000 + i, 5000);
        }
        TransactionQueryService svc = service();

        PageResult<TransactionSummary> page0 = svc.query(new TransactionFilter(null, null, 0, 10));
        PageResult<TransactionSummary> page1 = svc.query(new TransactionFilter(null, null, 1, 10));
        PageResult<TransactionSummary> page2 = svc.query(new TransactionFilter(null, null, 2, 10));

        assertEquals(25, page0.getTotalItems());
        assertEquals(3, page0.getTotalPages());
        assertEquals(10, page0.getItems().size());
        assertEquals(10, page1.getItems().size());
        assertEquals(5, page2.getItems().size());

        List<String> all = new ArrayList<>();
        page0.getItems().forEach(s -> all.add(s.getTransactionId()));
        page1.getItems().forEach(s -> all.add(s.getTransactionId()));
        page2.getItems().forEach(s -> all.add(s.getTransactionId()));
        // 同 updatedAt 时按事务 ID 升序兜底，无重复无遗漏
        List<String> expected = all.stream().sorted().toList();
        assertEquals(expected, all);
        assertEquals(25, all.stream().distinct().count());
        log.info("paged ids: {}", all);
    }

    @Test
    void thousandsOfTransactionsPaginateWithBoundedPage() {
        long base = 1_000_000L;
        for (int i = 0; i < 1200; i++) {
            String type = i % 3 == 0 ? "payment" : "order";
            persist(String.format("bulk-%05d", i), type,
                    i % 4 == 0 ? TransactionStatus.RUNNING : TransactionStatus.COMPLETED,
                    FailureReason.NONE, null, base + i, base + i);
        }
        TransactionQueryService svc = service();

        PageResult<TransactionSummary> page = svc.query(
                new TransactionFilter(TransactionStatus.RUNNING, null, 2, 50));
        // 1200 / 4 = 300 条 RUNNING
        assertEquals(300, page.getTotalItems());
        assertEquals(6, page.getTotalPages());
        assertEquals(50, page.getItems().size());
        // 倒序排列中 page=2（偏移 100）的第一条：1196 - 4*100 = 796
        assertEquals("bulk-00796", page.getItems().get(0).getTransactionId());
        assertTrue(page.getItems().stream()
                .allMatch(s -> s.getStatus() == TransactionStatus.RUNNING));

        // 页大小被钳制，不会一次返回上千条
        PageResult<TransactionSummary> huge = svc.query(
                new TransactionFilter(null, null, 0, 1_000_000));
        assertEquals(TransactionFilter.MAX_PAGE_SIZE, huge.getItems().size());

        // 摘要行不包含步骤/轨迹：内容字段齐全即可
        TransactionSummary any = huge.getItems().get(0);
        assertNotNull(any.getTransactionId());
        assertNotNull(any.getSagaType());
        assertNotNull(any.getStatus());
        log.info("bulk query: totalRunning=300, clampedPageSize={}, first={}",
                huge.getItems().size(), any.getTransactionId());
    }
}
