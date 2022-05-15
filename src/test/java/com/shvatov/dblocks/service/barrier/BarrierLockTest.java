package com.shvatov.dblocks.service.barrier;

import com.shvatov.dblocks.service.AbstractContainerTest;
import lombok.SneakyThrows;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BarrierLockTest extends AbstractContainerTest {
    private static final int BATCH_SIZE = 3;

    @Autowired
    private BarrierLockService barrierLockService;

    @SneakyThrows
    @RepeatedTest(10)
    @DisplayName("barrier awaits until required number of processes is present")
    void testBasic() {
        final var barrier = new CyclicBarrier(3);
        final var processIdentifier = uniqueProcessIdentifier();
        final var threadCompletionQueue = Collections.synchronizedCollection(new LinkedHashSet<Integer>());
        final var syncs = new ArrayList<Future<Object>>();

        syncs.add(
                executeInThread(() -> {
                    barrier.await();
                    barrierLockService.lockAndExecuteBatch(
                            processIdentifier, BATCH_SIZE,
                            () -> {
                                threadCompletionQueue.add(1);
                                return null;
                            });
                    return null;
                })
        );

        syncs.add(
                executeInThread(() -> {
                    barrier.await();
                    barrierLockService.lockAndExecuteBatch(
                            processIdentifier, BATCH_SIZE,
                            () -> {
                                threadCompletionQueue.add(2);
                                return null;
                            });
                    return null;
                })
        );

        syncs.add(
                executeInThread(() -> {
                    barrier.await();
                    barrierLockService.lockAndExecuteBatch(
                            processIdentifier, BATCH_SIZE,
                            () -> {
                                threadCompletionQueue.add(3);
                                return null;
                            });
                    return null;
                })
        );

        for (final Future<Object> sync : syncs) {
            sync.get();
        }

        assertTrue(threadCompletionQueue.contains(1));
        assertTrue(threadCompletionQueue.contains(2));
        assertTrue(threadCompletionQueue.contains(3));
    }

    @SneakyThrows
    @RepeatedTest(10)
    @DisplayName("batch size is 3, but 4 suppliers present => 1 master with 2 slaves, 1 master with no slaves")
    void testComplex1() {
        final var barrier = new CyclicBarrier(4);
        final var processIdentifier = uniqueProcessIdentifier();
        final var threadCompletionQueue = Collections.synchronizedCollection(new LinkedHashSet<Integer>());
        final var syncs = new ArrayList<Future<Object>>();

        syncs.add(
                executeInThread(() -> {
                    barrier.await();
                    barrierLockService.lockAndExecuteBatch(
                            processIdentifier, BATCH_SIZE,
                            () -> {
                                threadCompletionQueue.add(1);
                                return null;
                            });
                    return null;
                })
        );

        syncs.add(
                executeInThread(() -> {
                    barrier.await();
                    barrierLockService.lockAndExecuteBatch(
                            processIdentifier, BATCH_SIZE,
                            () -> {
                                threadCompletionQueue.add(2);
                                return null;
                            });
                    return null;
                })
        );

        syncs.add(
                executeInThread(() -> {
                    barrier.await();
                    barrierLockService.lockAndExecuteBatch(
                            processIdentifier, BATCH_SIZE,
                            () -> {
                                threadCompletionQueue.add(3);
                                return null;
                            });
                    return null;
                })
        );

        syncs.add(
                executeInThread(() -> {
                    barrier.await();
                    barrierLockService.lockAndExecuteBatch(
                            processIdentifier, BATCH_SIZE,
                            () -> {
                                threadCompletionQueue.add(4);
                                return null;
                            });
                    return null;
                })
        );

        final var exceptions = new ArrayList<ExecutionException>();
        for (final Future<Object> sync : syncs) {
            try {
                sync.get();
            } catch (final ExecutionException exception) {
                exceptions.add(exception);
            }
        }

        assertEquals(3, threadCompletionQueue.size());
        assertEquals(1, exceptions.size());
    }

    @SneakyThrows
    @RepeatedTest(10)
    @DisplayName("batch size is 2, 4 suppliers present => 2 masters with 2 slaves")
    void testComplex2() {
        final var barrier = new CyclicBarrier(4);
        final var processIdentifier = uniqueProcessIdentifier();
        final var threadCompletionQueue = Collections.synchronizedCollection(new LinkedHashSet<Integer>());
        final var syncs = new ArrayList<Future<Object>>();

        syncs.add(
                executeInThread(() -> {
                    barrier.await();
                    barrierLockService.lockAndExecuteBatch(
                            processIdentifier, 2,
                            () -> {
                                threadCompletionQueue.add(1);
                                return null;
                            });
                    return null;
                })
        );

        syncs.add(
                executeInThread(() -> {
                    barrier.await();
                    barrierLockService.lockAndExecuteBatch(
                            processIdentifier, 2,
                            () -> {
                                threadCompletionQueue.add(2);
                                return null;
                            });
                    return null;
                })
        );

        syncs.add(
                executeInThread(() -> {
                    barrier.await();
                    barrierLockService.lockAndExecuteBatch(
                            processIdentifier, 2,
                            () -> {
                                threadCompletionQueue.add(3);
                                return null;
                            });
                    return null;
                })
        );

        syncs.add(
                executeInThread(() -> {
                    barrier.await();
                    barrierLockService.lockAndExecuteBatch(
                            processIdentifier, 2,
                            () -> {
                                threadCompletionQueue.add(4);
                                return null;
                            });
                    return null;
                })
        );

        for (final Future<Object> sync : syncs) {
            sync.get();
        }

        assertTrue(threadCompletionQueue.contains(1));
        assertTrue(threadCompletionQueue.contains(2));
        assertTrue(threadCompletionQueue.contains(3));
        assertTrue(threadCompletionQueue.contains(4));
    }
}