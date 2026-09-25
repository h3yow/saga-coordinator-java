package com.github.h3yow.sagacoordinatorjava.tests.spring;

import com.github.h3yow.sagacoordinatorjava.model.SagaDefinition;
import com.github.h3yow.sagacoordinatorjava.saga.SagaRegistry;
import com.github.h3yow.sagacoordinatorjava.tests.support.TestContext;
import com.github.h3yow.sagacoordinatorjava.tests.support.TestSagaFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Supplier;

/**
 * 集成测试用：在 Spring 上下文中注册标准三步 Saga。
 */
@Configuration
public class SagaTestBeans {

    /** 恢复时使用的上下文工厂：每次推进创建干净上下文 */
    @Bean
    public Supplier<TestContext> testContextSupplier() {
        return TestContext::new;
    }

    @Bean
    public SagaDefinition<TestContext> orderSagaDefinition(SagaRegistry registry,
                                                           Supplier<TestContext> supplier) {
        SagaDefinition<TestContext> def = TestSagaFactory.threeStepDefinition();
        registry.register(def, supplier);
        return def;
    }
}
