package com.github.h3yow.sagacoordinatorjava.coordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 进程启动后恢复未完成事务，并周期性巡检超时事务。
 * 通过 saga.recovery.enabled 开关控制（默认开启）。
 */
@Component
public class RecoveryScheduler {

    private static final Logger log = LoggerFactory.getLogger(RecoveryScheduler.class);

    private final RecoveryService recoveryService;

    @Value("${saga.recovery.enabled:true}")
    private boolean enabled;

    public RecoveryScheduler(RecoveryService recoveryService) {
        this.recoveryService = recoveryService;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onStartup() {
        if (!enabled) {
            return;
        }
        log.info("startup recovery begins");
        recoveryService.recoverAll();
    }

    @Scheduled(fixedDelayString = "${saga.recovery.sweep-interval-millis:5000}")
    public void sweep() {
        if (!enabled) {
            return;
        }
        recoveryService.sweepTimeouts(System.currentTimeMillis());
    }
}
