package com.github.h3yow.sagacoordinatorjava.web;

import com.github.h3yow.sagacoordinatorjava.coordinator.SagaCoordinator;
import com.github.h3yow.sagacoordinatorjava.model.TransactionResult;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 演示用查询入口：读取事务的可解释结论与状态迁移轨迹。
 * 事务提交由业务侧通过 {@link SagaCoordinator#submit} 完成（需携带业务上下文）。
 */
@RestController
@RequestMapping("/saga/transactions")
public class SagaController {

    private final SagaCoordinator coordinator;

    public SagaController(SagaCoordinator coordinator) {
        this.coordinator = coordinator;
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
}
