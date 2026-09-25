package com.github.h3yow.sagacoordinatorjava.saga.web;

import com.github.h3yow.sagacoordinatorjava.saga.engine.SagaCoordinator;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 只读查询入口（提交在单进程测试中直接使用协调器 API）。
 */
@RestController
@RequestMapping("/api/sagas")
public class SagaController {

    private final SagaCoordinator coordinator;

    public SagaController(SagaCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @GetMapping("/{sagaId}")
    public SagaResult getStatus(@PathVariable String sagaId) {
        return coordinator.getStatus(sagaId);
    }
}
