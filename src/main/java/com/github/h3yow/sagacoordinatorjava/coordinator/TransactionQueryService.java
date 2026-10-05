package com.github.h3yow.sagacoordinatorjava.coordinator;

import com.github.h3yow.sagacoordinatorjava.model.PageResult;
import com.github.h3yow.sagacoordinatorjava.model.TransactionFilter;
import com.github.h3yow.sagacoordinatorjava.model.TransactionSummary;
import com.github.h3yow.sagacoordinatorjava.store.TransactionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 事务总览查询：按状态 / 业务类型筛选，按最近进展时间倒序分页返回。
 *
 * <p>只读、不获取任何事务锁，因此查询不会阻塞或干扰正在推进的事务；
 * 存储侧以流式摘要扫描，内存中只保留摘要（不含步骤明细与事件轨迹）。
 */
public class TransactionQueryService {

    private static final Logger log = LoggerFactory.getLogger(TransactionQueryService.class);

    private final TransactionStore store;

    public TransactionQueryService(TransactionStore store) {
        this.store = store;
    }

    /**
     * 分页查询事务摘要。页号从 0 开始；单页大小被限制在
     * {@link TransactionFilter#MAX_PAGE_SIZE} 以内。
     */
    public PageResult<TransactionSummary> query(TransactionFilter filter) {
        TransactionFilter f = normalize(filter);
        List<TransactionSummary> matched = new ArrayList<>();
        // 流式扫描 + 就地过滤：不加载完整事务聚合
        store.scanSummaries(summary -> {
            if (matches(summary, f)) {
                matched.add(summary);
            }
        });
        // 最近进展优先；同一时间戳用事务 ID 兜底，保证排序确定、分页稳定
        matched.sort(Comparator
                .comparingLong(TransactionSummary::getUpdatedAt).reversed()
                .thenComparing(TransactionSummary::getTransactionId));

        long total = matched.size();
        int fromIndex = (int) Math.min((long) f.getPage() * f.getSize(), total);
        int toIndex = (int) Math.min(fromIndex + (long) f.getSize(), total);
        List<TransactionSummary> page = new ArrayList<>(matched.subList(fromIndex, toIndex));
        log.debug("transaction query status={} sagaType={} page={} size={} total={} returned={}",
                f.getStatus(), f.getSagaType(), f.getPage(), f.getSize(), total, page.size());
        return new PageResult<>(page, f.getPage(), f.getSize(), total);
    }

    private boolean matches(TransactionSummary summary, TransactionFilter filter) {
        if (filter.getStatus() != null && summary.getStatus() != filter.getStatus()) {
            return false;
        }
        if (filter.getSagaType() != null
                && !filter.getSagaType().equals(summary.getSagaType())) {
            return false;
        }
        return true;
    }

    private TransactionFilter normalize(TransactionFilter filter) {
        TransactionFilter f = filter != null ? filter : new TransactionFilter();
        int page = Math.max(f.getPage(), 0);
        int size = f.getSize();
        if (size <= 0) {
            size = 20;
        }
        size = Math.min(size, TransactionFilter.MAX_PAGE_SIZE);
        String sagaType = f.getSagaType();
        if (sagaType != null && sagaType.isBlank()) {
            sagaType = null;
        }
        return new TransactionFilter(f.getStatus(), sagaType, page, size);
    }
}
