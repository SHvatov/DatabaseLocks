package com.shvatov.dblocks.service.readwrite;

import com.shvatov.dblocks.service.AbstractContainerTest;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.concurrent.CyclicBarrier;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class ReadWriteLockPerformanceTest extends AbstractContainerTest {
    @Autowired
    private ReadWriteLockService readWriteLockService;

    @Autowired
    private PgReadWriteLockService pgReadWriteLockService;

    @SneakyThrows
    @RepeatedTest(10)
    @DisplayName("compare the performance before warm up")
    void testWarmUpPerformance() {
        final var barrier = new CyclicBarrier(2);
        final var processIdentifier = uniqueProcessIdentifier();
        final var threadToCompletionTime = new HashMap<Integer, Long>();

        final var sync1 = executeInThread(() ->
                executeInTransaction(() -> {
                    barrier.await(); // sync point
                    final var result = runMeasuringTime(
                            () -> {
                                readWriteLockService.acquireExclusiveLock(processIdentifier);
                                return null;
                            }
                    );
                    threadToCompletionTime.put(1, result.executionTime());
                    return null;
                })
        );

        final var sync2 = executeInThread(() ->
                executeInTransaction(() -> {
                    barrier.await(); // sync point
                    final var result = runMeasuringTime(
                            () -> {
                                pgReadWriteLockService.acquireExclusiveLock(processIdentifier);
                                return null;
                            }
                    );
                    threadToCompletionTime.put(2, result.executionTime());
                    return null;
                })
        );

        // wait for the threads to complete
        sync1.get();
        sync2.get();

        assertTrue(threadToCompletionTime.containsKey(1));
        assertTrue(threadToCompletionTime.containsKey(2));

        log.info("Time spent using artificial locks: {} ns", threadToCompletionTime.get(1));
        log.info("Time spent using pg locks: {} ns", threadToCompletionTime.get(2));
    }

    @SneakyThrows
    @RepeatedTest(10)
    @DisplayName("compare the performance after warm up")
    void testWarmedUpPerformance() {
        final var barrier = new CyclicBarrier(2);
        final var processIdentifier = uniqueProcessIdentifier();
        final var threadToCompletionTime = new HashMap<Integer, Long>();

        // insert lock in the DB
        executeInTransaction(() -> {
            readWriteLockService.acquireExclusiveLock(processIdentifier);
            return null;
        });
        executeInTransaction(() -> {
            pgReadWriteLockService.acquireExclusiveLock(processIdentifier);
            return null;
        });

        final var sync1 = executeInThread(() ->
                executeInTransaction(() -> {
                    barrier.await(); // sync point
                    final var result = runMeasuringTime(
                            () -> {
                                readWriteLockService.acquireExclusiveLock(processIdentifier);
                                return null;
                            }
                    );
                    threadToCompletionTime.put(1, result.executionTime());
                    return null;
                })
        );

        final var sync2 = executeInThread(() ->
                executeInTransaction(() -> {
                    barrier.await(); // sync point
                    final var result = runMeasuringTime(
                            () -> {
                                pgReadWriteLockService.acquireExclusiveLock(processIdentifier);
                                return null;
                            }
                    );
                    threadToCompletionTime.put(2, result.executionTime());
                    return null;
                })
        );

        // wait for the threads to complete
        sync1.get();
        sync2.get();

        assertTrue(threadToCompletionTime.containsKey(1));
        assertTrue(threadToCompletionTime.containsKey(2));

        log.info("Time spent using artificial locks: {} ns", threadToCompletionTime.get(1));
        log.info("Time spent using pg locks: {} ns", threadToCompletionTime.get(2));
    }
}