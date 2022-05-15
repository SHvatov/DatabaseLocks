package com.shvatov.dblocks.service.readwrite;

import com.shvatov.dblocks.service.AbstractContainerTest;
import lombok.SneakyThrows;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ReadWriteLockTest extends AbstractContainerTest {
    @Autowired
    private ReadWriteLockService readWriteLockService;

    @Autowired
    private PgReadWriteLockService pgReadWriteLockService;

    private static final AtomicInteger executionCounter = new AtomicInteger(0);

    private AbstractReadWriteLockService lockService() {
        final var executionNumber = executionCounter.getAndIncrement();
        if (executionNumber % 2 == 0) {
            return readWriteLockService;
        }
        return pgReadWriteLockService;
    }

    @SneakyThrows
    @RepeatedTest(20)
    @DisplayName("acquire exclusive lock")
    void testExclusiveLock() {
        final var barrier = new CyclicBarrier(2);
        final var processIdentifier = uniqueProcessIdentifier();
        final var threadToCompletionTime = new ConcurrentHashMap<Integer, LocalDateTime>();
        final var service = lockService();

        final var sync1 = executeInThread(() ->
                executeInTransaction(() -> {
                    barrier.await(); // sync point
                    Thread.sleep(100); // ensure, that this process will be second to acquire the lock
                    service.acquireExclusiveLock(processIdentifier);
                    threadToCompletionTime.put(1, LocalDateTime.now());
                    return null;
                })
        );

        final var sync2 = executeInThread(() ->
                executeInTransaction(() -> {
                    barrier.await(); // sync point
                    service.acquireExclusiveLock(processIdentifier);
                    Thread.sleep(500); // emulate some processing
                    threadToCompletionTime.put(2, LocalDateTime.now());
                    return null;
                })
        );

        // wait for the threads to complete
        sync1.get();
        sync2.get();

        assertTrue(threadToCompletionTime.containsKey(1));
        assertTrue(threadToCompletionTime.containsKey(2));
        assertTrue(threadToCompletionTime.get(1).compareTo(threadToCompletionTime.get(2)) > 0);
    }

    @SneakyThrows
    @RepeatedTest(20)
    @DisplayName("acquire shared lock with no blocks")
    void testSharedLock() {
        final var barrier = new CyclicBarrier(2);
        final var processIdentifier = uniqueProcessIdentifier();
        final var threadToCompletionTime = new ConcurrentHashMap<Integer, LocalDateTime>();
        final var service = lockService();

        final var sync1 = executeInThread(() ->
                executeInTransaction(() -> {
                    barrier.await(); // sync point
                    Thread.sleep(100); // ensure, that this process will be second to acquire the lock
                    service.acquireSharedLock(processIdentifier);
                    threadToCompletionTime.put(1, LocalDateTime.now());
                    return null;
                })
        );

        final var sync2 = executeInThread(() ->
                executeInTransaction(() -> {
                    barrier.await(); // sync point
                    service.acquireSharedLock(processIdentifier);
                    Thread.sleep(500); // emulate some processing
                    threadToCompletionTime.put(2, LocalDateTime.now());
                    return null;
                })
        );

        // wait for the threads to complete
        sync1.get();
        sync2.get();

        assertTrue(threadToCompletionTime.containsKey(1));
        assertTrue(threadToCompletionTime.containsKey(2));
        assertTrue(threadToCompletionTime.get(2).compareTo(threadToCompletionTime.get(1)) > 0);
    }

    @SneakyThrows
    @RepeatedTest(20)
    @DisplayName("first acquire shared locks, then attempt to acquire exclusive lock - exclusive lock awaits")
    void testComplex1() {
        final var barrier = new CyclicBarrier(3);
        final var processIdentifier = uniqueProcessIdentifier();
        final var threadToCompletionTime = new ConcurrentHashMap<Integer, LocalDateTime>();
        final var service = lockService();

        final var sync1 = executeInThread(() ->
                executeInTransaction(() -> {
                    barrier.await(); // sync point
                    service.acquireSharedLock(processIdentifier);
                    Thread.sleep(500);
                    threadToCompletionTime.put(2, LocalDateTime.now());
                    return null;
                })
        );

        final var sync2 = executeInThread(() ->
                executeInTransaction(() -> {
                    barrier.await(); // sync point
                    service.acquireSharedLock(processIdentifier);
                    Thread.sleep(300);
                    threadToCompletionTime.put(1, LocalDateTime.now());
                    return null;
                })
        );

        final var sync3 = executeInThread(() ->
                executeInTransaction(() -> {
                    barrier.await(); // sync point
                    Thread.sleep(100); // make sure this thread is second
                    service.acquireExclusiveLock(processIdentifier);
                    threadToCompletionTime.put(3, LocalDateTime.now());
                    return null;
                })
        );

        // wait for the threads to complete
        sync1.get();
        sync2.get();
        sync3.get();

        assertTrue(threadToCompletionTime.containsKey(1));
        assertTrue(threadToCompletionTime.containsKey(2));
        assertTrue(threadToCompletionTime.containsKey(3));
        assertTrue(threadToCompletionTime.get(1).compareTo(threadToCompletionTime.get(2)) < 0);
        assertTrue(threadToCompletionTime.get(2).compareTo(threadToCompletionTime.get(3)) < 0);
    }

    @SneakyThrows
    @RepeatedTest(20)
    @DisplayName("create locks simultaneously")
    void testComplex2() {
        final var barrier = new CyclicBarrier(2);
        final var processIdentifier = uniqueProcessIdentifier();
        final var threadToCompletionTime = new ConcurrentHashMap<Integer, LocalDateTime>();
        final var service = lockService();

        final var sync1 = executeInThread(() ->
                executeInTransaction(() -> {
                    barrier.await(); // sync point
                    service.acquireSharedLock(processIdentifier);
                    Thread.sleep(500);
                    threadToCompletionTime.put(2, LocalDateTime.now());
                    return null;
                })
        );

        final var sync2 = executeInThread(() ->
                executeInTransaction(() -> {
                    barrier.await(); // sync point
                    service.acquireSharedLock(processIdentifier);
                    Thread.sleep(300);
                    threadToCompletionTime.put(1, LocalDateTime.now());
                    return null;
                })
        );

        // wait for the threads to complete
        sync1.get();
        sync2.get();

        assertTrue(threadToCompletionTime.containsKey(1));
        assertTrue(threadToCompletionTime.containsKey(2));
    }
}