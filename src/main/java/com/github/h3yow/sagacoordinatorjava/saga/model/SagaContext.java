package com.github.h3yow.sagacoordinatorjava.saga.model;

import java.util.HashMap;
import java.util.Map;

/**
 * 事务级上下文：在各参与者步骤之间传递业务数据。
 */
public final class SagaContext {

    private final String sagaId;
    private final Map<String, String> attributes = new HashMap<>();

    public SagaContext(String sagaId) {
        this.sagaId = sagaId;
    }

    public String getSagaId() {
        return sagaId;
    }

    public void put(String key, String value) {
        attributes.put(key, value);
    }

    public String get(String key) {
        return attributes.get(key);
    }

    public Map<String, String> snapshot() {
        return Map.copyOf(attributes);
    }
}
