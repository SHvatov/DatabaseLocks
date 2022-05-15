package com.shvatov.dblocks.service;

import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.support.TestPropertySourceUtils;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@DataJpaTest
@Testcontainers
@ActiveProfiles("test")
@ComponentScan("com.shvatov.dblocks.service")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ContextConfiguration(initializers = AbstractContainerTest.DockerPostgreDataSourceInitializer.class)
public abstract class AbstractContainerTest {
    @SuppressWarnings("rawtypes")
    private static final PostgreSQLContainer postgreDBContainer = new PostgreSQLContainer<>("postgres:latest");

    @Autowired
    private TransactionalProcessor transactionalProcessor;

    private final ExecutorService executor = Executors.newCachedThreadPool();

    static {
        postgreDBContainer.setCommand("postgres", "-c", "max_connections=100");
        postgreDBContainer.start();
    }

    public static class DockerPostgreDataSourceInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(@NotNull final ConfigurableApplicationContext applicationContext) {
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
                    applicationContext,
                    "spring.datasource.url=" + postgreDBContainer.getJdbcUrl(),
                    "spring.datasource.username=" + postgreDBContainer.getUsername(),
                    "spring.datasource.password=" + postgreDBContainer.getPassword()
            );
        }
    }

    public static record MeasuredExecResult<R>(R result, Long executionTime) {}

    @SneakyThrows
    protected <R> Future<R> executeInThread(final Callable<R> action) {
        return executor.submit(action);
    }

    @SneakyThrows
    protected <R> MeasuredExecResult<R> runMeasuringTime(final Callable<R> action) {
        final var start = System.nanoTime();
        final var result = action.call();
        final var spentTime = System.nanoTime() - start;
        return new MeasuredExecResult<>(result, spentTime);
    }

    @SneakyThrows
    protected <R> R executeInTransaction(final Callable<R> action) {
        return transactionalProcessor.execute(action);
    }

    protected String uniqueProcessIdentifier() {
        return UUID.randomUUID().toString();
    }
}
