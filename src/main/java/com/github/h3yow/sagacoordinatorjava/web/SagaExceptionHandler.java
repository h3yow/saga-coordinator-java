package com.github.h3yow.sagacoordinatorjava.web;

import com.github.h3yow.sagacoordinatorjava.coordinator.CapacityExceededException;
import com.github.h3yow.sagacoordinatorjava.coordinator.ManualActionRejectedException;
import com.github.h3yow.sagacoordinatorjava.model.IllegalTransitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * 把协调器领域异常映射成稳定的 HTTP 语义：
 * 请求错误（事务不存在 / 处置被拒绝 / 非法迁移）→ 4xx，且不吞掉原因；
 * 容量已满（REJECT）→ 429，调用方可稍后重试。
 */
@RestControllerAdvice
public class SagaExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(SagaExceptionHandler.class);

    @ExceptionHandler(CapacityExceededException.class)
    public ResponseEntity<Map<String, String>> handleCapacity(CapacityExceededException e) {
        log.warn("capacity exceeded: {}", e.getMessage());
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .body(Map.of("error", "CAPACITY_EXCEEDED", "message", e.getMessage()));
    }

    @ExceptionHandler(ManualActionRejectedException.class)
    public ResponseEntity<Map<String, String>> handleRejected(ManualActionRejectedException e) {
        log.warn("manual action rejected: {}", e.getMessage());
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Map.of("error", "MANUAL_ACTION_REJECTED", "message", e.getMessage()));
    }

    @ExceptionHandler({IllegalArgumentException.class, IllegalTransitionException.class})
    public ResponseEntity<Map<String, String>> handleBadRequest(RuntimeException e) {
        log.warn("bad saga request: {}", e.getMessage());
        return ResponseEntity.badRequest()
                .body(Map.of("error", "BAD_REQUEST", "message", e.getMessage()));
    }
}
