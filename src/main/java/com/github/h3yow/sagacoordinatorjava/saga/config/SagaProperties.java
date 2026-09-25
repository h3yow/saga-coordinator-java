package com.github.h3yow.sagacoordinatorjava.saga.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * 协调器配置（application.properties 前缀 saga）。
 */
@ConfigurationProperties(prefix = "saga")
public class SagaProperties {

    /** 事件日志目录。 */
    private String storeDir = "build/saga-eventlog";

    /** 超时巡检间隔。 */
    private Duration reaperInterval = Duration.ofSeconds(2);

    /** 事务无进展多久视为挂起/超时。 */
    private Duration staleThreshold = Duration.ofSeconds(10);

    public String getStoreDir() {
        return storeDir;
    }

    public void setStoreDir(String storeDir) {
        this.storeDir = storeDir;
    }

    public Duration getReaperInterval() {
        return reaperInterval;
    }

    public void setReaperInterval(Duration reaperInterval) {
        this.reaperInterval = reaperInterval;
    }

    public Duration getStaleThreshold() {
        return staleThreshold;
    }

    public void setStaleThreshold(Duration staleThreshold) {
        this.staleThreshold = staleThreshold;
    }
}
