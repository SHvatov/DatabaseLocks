package com.shvatov.dblocks.service.barrier;

import com.shvatov.dblocks.model.enums.SyncResult;
import com.shvatov.dblocks.service.TransactionalProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.Callable;

@Slf4j
@Service
@RequiredArgsConstructor
public class BarrierLockService {
    private final TransactionalProcessor transactionalProcessor;
    private final MasterLockProcessor masterLockProcessor;
    private final SlaveLockProcessor slaveLockProcessor;


    @Transactional(propagation = Propagation.NEVER)
    public <T> T lockAndExecuteBatch(final String processIdentifier,
                                     final int batchSize,
                                     final Callable<T> action) throws Exception {
        transactionalProcessor.process(() -> masterLockProcessor.createMasterLock(processIdentifier));
        final boolean isProcessedAsMaster = transactionalProcessor.execute(
                () -> masterLockProcessor.attemptToAcquireMasterLock(processIdentifier)
                        .map(masterLock -> {
                            final var result = masterLockProcessor.process(processIdentifier, batchSize);
                            if (SyncResult.SUCCESS != result) {
                                throw new IllegalStateException(
                                        "Failed to process master lock for process %s"
                                                .formatted(processIdentifier)
                                );
                            }
                            return true;
                        }).orElse(false)
        );

        if (!isProcessedAsMaster) {
            final var result = slaveLockProcessor.process(processIdentifier);
            if (result == SyncResult.FAILURE) {
                throw new IllegalStateException(
                        "Failed to process slave lock for process %s"
                                .formatted(processIdentifier)
                );
            } else if (result == SyncResult.REPEAT_REQUIRED) {
                return lockAndExecuteBatch(processIdentifier, batchSize, action);
            }
        }
        return action.call();
    }
}
