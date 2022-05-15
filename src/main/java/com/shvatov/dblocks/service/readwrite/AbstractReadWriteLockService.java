package com.shvatov.dblocks.service.readwrite;


import com.shvatov.dblocks.model.enums.LockMode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@RequiredArgsConstructor
public abstract class AbstractReadWriteLockService {
    protected static final String PROCESS_IDENTIFIER_PARAM_NAME = "processIdentifier";

    protected final NamedParameterJdbcOperations jdbcTemplate;

    @Transactional(propagation = Propagation.MANDATORY, isolation = Isolation.READ_COMMITTED)
    public void acquireExclusiveLock(final String processIdentifier) {
        doAcquireLock(processIdentifier, LockMode.EXCLUSIVE);
    }

    @Transactional(propagation = Propagation.MANDATORY, isolation = Isolation.READ_COMMITTED)
    public void acquireSharedLock(final String processIdentifier) {
        doAcquireLock(processIdentifier, LockMode.SHARED);
    }

    protected abstract void doAcquireLock(final String processIdentifier, final LockMode mode);
}
