package com.shvatov.dblocks.service.readwrite;

import com.shvatov.dblocks.model.enums.LockMode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.stereotype.Service;

import java.sql.PreparedStatement;

@Slf4j
@Service
public class PgReadWriteLockService extends AbstractReadWriteLockService {
    public PgReadWriteLockService(final NamedParameterJdbcOperations jdbcTemplate) {
        super(jdbcTemplate);
    }

    @Override
    protected void doAcquireLock(final String processIdentifier, final LockMode mode) {
        log.info(
                "Attempting to acquire lock for the process with id = {} with mode = {} using pg procedures",
                processIdentifier, mode
        );
        jdbcTemplate.execute(
                "select %s(:%s)".formatted(
                        mode.getPgLockFunction(),
                        PROCESS_IDENTIFIER_PARAM_NAME
                ),
                new MapSqlParameterSource().addValue(PROCESS_IDENTIFIER_PARAM_NAME, processIdentifier.hashCode()),
                PreparedStatement::execute
        );
    }
}
