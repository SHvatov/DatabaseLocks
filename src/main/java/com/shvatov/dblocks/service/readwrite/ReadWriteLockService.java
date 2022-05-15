package com.shvatov.dblocks.service.readwrite;

import com.shvatov.dblocks.model.ReadWriteLock;
import com.shvatov.dblocks.model.enums.LockMode;
import com.shvatov.dblocks.service.TransactionalProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@Service
public class ReadWriteLockService extends AbstractReadWriteLockService {
    private final TransactionalProcessor transactionalProcessor;

    public ReadWriteLockService(final NamedParameterJdbcOperations jdbcTemplate,
                                final TransactionalProcessor transactionalProcessor) {
        super(jdbcTemplate);
        this.transactionalProcessor = transactionalProcessor;
    }

    @Override
    protected void doAcquireLock(final String processIdentifier, final LockMode mode) {
        final var existingLock = attemptToAcquireLock(processIdentifier, mode);
        if (existingLock.isPresent()) {
            log.info("Acquired existing lock for the process \"{}\" with mode \"{}\"", processIdentifier, mode);
            return;
        }

        try {
            log.info("Attempting to create a lock for the process \"{}\"", processIdentifier);
            transactionalProcessor.process(() -> // optional, no difference where we wait - on insert or on select
                    jdbcTemplate.update(
                            "insert into %s(%s) values (:%s)".formatted(
                                    ReadWriteLock.TABLE_NAME,
                                    ReadWriteLock.PROCESS_IDENTIFIER_COLUMN_NAME,
                                    PROCESS_IDENTIFIER_PARAM_NAME
                            ),
                            new MapSqlParameterSource().addValue(PROCESS_IDENTIFIER_PARAM_NAME, processIdentifier)
                    )
            );
            log.info("Created a lock for the process \"{}\"", processIdentifier);
        } catch (final DataAccessException exception) {
            if (!(exception instanceof DuplicateKeyException)) {
                throw exception;
            }
            log.info(
                    "Unique constraint violation exception caught " +
                            "while creating a lock for the process \"{}\"", processIdentifier
            );
        }

        attemptToAcquireLock(processIdentifier, mode)
                .orElseThrow(() ->
                        new IllegalStateException(
                                ("Could not obtain lock for the process \"%s\"")
                                        .formatted(processIdentifier)
                        )
                );
        log.info("Acquired existing lock for the process \"{}\" with mode \"{}\"", processIdentifier, mode);
    }

    private Optional<ReadWriteLock> attemptToAcquireLock(final String processIdentifier, final LockMode mode) {
        return jdbcTemplate.queryForStream(
                "select * from %s where %s = :%s for %s".formatted(
                        ReadWriteLock.TABLE_NAME,
                        ReadWriteLock.PROCESS_IDENTIFIER_COLUMN_NAME,
                        PROCESS_IDENTIFIER_PARAM_NAME,
                        mode.getSqlKeyWord()
                ),
                new MapSqlParameterSource().addValue(PROCESS_IDENTIFIER_PARAM_NAME, processIdentifier),
                (rs, rowNum) -> new ReadWriteLock(rs.getString(1), mode)
        ).findFirst();
    }
}
