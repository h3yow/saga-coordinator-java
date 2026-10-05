package com.github.h3yow.sagacoordinatorjava.web;

import com.github.h3yow.sagacoordinatorjava.coordinator.SagaCoordinator;
import com.github.h3yow.sagacoordinatorjava.coordinator.TransactionQueryService;
import com.github.h3yow.sagacoordinatorjava.model.PageResult;
import com.github.h3yow.sagacoordinatorjava.model.TransactionFilter;
import com.github.h3yow.sagacoordinatorjava.model.TransactionResult;
import com.github.h3yow.sagacoordinatorjava.model.TransactionStatus;
import com.github.h3yow.sagacoordinatorjava.model.TransactionSummary;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 演示用查询入口：读取事务的可解释结论与状态迁移轨迹。
 * 事务提交由业务侧通过 {@link SagaCoordinator#submit} 完成（需携带业务上下文）。
 */
@RestController
@RequestMapping("/saga/transactions")
public class SagaController {

    private final SagaCoordinator coordinator;
    private final TransactionQueryService queryService;

    public SagaController(SagaCoordinator coordinator, TransactionQueryService queryService) {
        this.coordinator = coordinator;
        this.queryService = queryService;
    }

    /**
     * 事务总览：可按状态、业务类型筛选并分页；按最近进展时间倒序返回。
     */
    @GetMapping
    public PageResult<TransactionSummary> list(
            @RequestParam(required = false) TransactionStatus status,
            @RequestParam(required = false) String sagaType,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        return queryService.query(new TransactionFilter(status, sagaType, page, size));
    }

    @GetMapping("/{transactionId}")
    public ResponseEntity<TransactionResult> get(@PathVariable String transactionId) {
        return coordinator.getResult(transactionId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/{transactionId}/recover")
    public ResponseEntity<TransactionResult> recover(@PathVariable String transactionId) {
        return ResponseEntity.ok(coordinator.recover(transactionId));
    }

    /**
     * 人工再试一次补偿（仅 COMPENSATION_FAILED 事务可发起；已收敛终态幂等返回）。
     */
    @PostMapping("/{transactionId}/manual-retry")
    public ResponseEntity<TransactionResult> manualRetry(@PathVariable String transactionId) {
        return ResponseEntity.ok(coordinator.retryCompensation(transactionId));
    }

    /**
     * 人工确认处理完毕。请求体可选：{"note": "处置说明"}，也可以为空。
     */
    @PostMapping("/{transactionId}/manual-resolve")
    public ResponseEntity<TransactionResult> manualResolve(
            @PathVariable String transactionId,
            @RequestBody(required = false) Map<String, String> body) {
        String note = body != null ? body.get("note") : null;
        return ResponseEntity.ok(coordinator.resolveManually(transactionId, note));
    }
}
