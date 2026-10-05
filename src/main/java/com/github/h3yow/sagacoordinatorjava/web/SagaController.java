package com.github.h3yow.sagacoordinatorjava.web;

import com.github.h3yow.sagacoordinatorjava.coordinator.ConcurrencyLimitExceededException;
import com.github.h3yow.sagacoordinatorjava.coordinator.InterventionRejectedException;
import com.github.h3yow.sagacoordinatorjava.coordinator.SagaCoordinator;
import com.github.h3yow.sagacoordinatorjava.coordinator.TransactionQueryService;
import com.github.h3yow.sagacoordinatorjava.model.InterventionAction;
import com.github.h3yow.sagacoordinatorjava.model.PageResult;
import com.github.h3yow.sagacoordinatorjava.model.TransactionResult;
import com.github.h3yow.sagacoordinatorjava.model.TransactionStatus;
import com.github.h3yow.sagacoordinatorjava.model.TransactionSummary;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 运维入口：事务总览分页查询、单事务结论查询、人工介入处置。
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
     * 事务总览：按状态 / 业务类型筛选，按最近进展时间倒序分页。
     */
    @GetMapping
    public PageResult<TransactionSummary> list(
            @RequestParam(required = false) TransactionStatus status,
            @RequestParam(required = false) String sagaType,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        return queryService.query(status, sagaType, page, size);
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
     * 人工介入处置：RETRY_COMPENSATION 再试一次补偿收尾；
     * MARK_RESOLVED 显式标记人工已处理完毕。
     */
    @PostMapping("/{transactionId}/intervention")
    public TransactionResult intervene(@PathVariable String transactionId,
                                       @RequestParam InterventionAction action) {
        return coordinator.intervene(transactionId, action);
    }

    @ExceptionHandler(InterventionRejectedException.class)
    public ResponseEntity<Map<String, String>> interventionRejected(InterventionRejectedException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(ConcurrencyLimitExceededException.class)
    public ResponseEntity<Map<String, String>> concurrencyLimit(ConcurrencyLimitExceededException e) {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }
}
