package com.shvatov.dblocks.service.barrier;

import com.shvatov.dblocks.model.MasterLock;
import com.shvatov.dblocks.model.enums.SyncResult;
import com.shvatov.dblocks.model.enums.SyncStatus;
import com.shvatov.dblocks.service.TransactionalProcessor;
import com.shvatov.dblocks.service.seq.SequenceValueGenerator;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class MasterLockProcessor {
    private static final int LOCK_OBTAINING_TIMEOUT = 1;
    private static final int POLLING_DELAY = 6000;
    private static final int POLLING_ATTEMPTS = 10;
    private static final int FAILED_ATTEMPTS_THRESHOLD = 3;

    private final SequenceValueGenerator sequenceValueGenerator;
    private final TransactionalProcessor transactionalProcessor;
    private final JdbcTemplate jdbcTemplate;

    @Transactional(propagation = Propagation.MANDATORY)
    public void createMasterLock(final String processIdentifier) {
        try {
            log.info("Attempting to register a master for the process with identifier {}", processIdentifier);
            jdbcTemplate.update(
                    "insert into db_master_lock(process_identifier, status) values(?, ?)",
                    ps -> {
                        ps.setString(1, processIdentifier);
                        ps.setString(2, SyncStatus.IN_PROGRESS.toString());
                    }
            );
            log.info("Registered a master for the process with identifier {}", processIdentifier);
        } catch (final DataAccessException exception) {
            if (!(exception instanceof DuplicateKeyException)) {
                throw exception;
            }
            log.info("Master for the process with identifier {} has already been registered", processIdentifier);
        }
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public Optional<MasterLock> attemptToAcquireMasterLock(final String processIdentifier) {
        log.info("Attempting to acquire master lock for the process with id = {}", processIdentifier);
        try {
            return jdbcTemplate.queryForStream(
                    """
                            select * from db_master_lock
                            where process_identifier = ?
                            and id is null for update""",
                    ps -> {
                        ps.setQueryTimeout(LOCK_OBTAINING_TIMEOUT);
                        ps.setString(1, processIdentifier);
                    },
                    (rs, rowNum) -> new MasterLock(
                            rs.getLong("id"),
                            rs.getString("process_identifier"),
                            SyncStatus.valueOf(rs.getString("status"))
                    )
            ).findFirst();
        } catch (final DataAccessException exception) {
            if (isQueryTimeoutException(exception)) {
                return Optional.empty();
            }
            throw exception;
        }
    }

    private boolean isQueryTimeoutException(final Throwable exception) {
        if (exception instanceof QueryTimeoutException) {
            return true;
        }

        if (exception.getCause() instanceof final SQLException sqlException) {
            return Objects.equals("57014", sqlException.getSQLState());
        }
        return false;
    }

    @SneakyThrows
    @Transactional(propagation = Propagation.MANDATORY)
    public SyncResult process(final String processIdentifier, final int batchSize) {
        log.info("Started the processing of the master for the process with identifier = {}", processIdentifier);

        final var masterId = sequenceValueGenerator.nextValue();
        final var failedAttemptsBySlaveId = new HashMap<Long, Integer>();
        for (int attempt = 0; attempt < POLLING_ATTEMPTS; attempt++) {
            final SyncResult result = attemptToSynchronize(masterId, processIdentifier, batchSize, failedAttemptsBySlaveId);
            if (SyncResult.REPEAT_REQUIRED == result) {
                Thread.sleep(POLLING_DELAY);
                continue;
            }
            return result;
        }

        onSyncFailure(masterId, processIdentifier, batchSize);
        return SyncResult.FAILURE;
    }

    private void onSyncFailure(final long masterId,
                               final String processIdentifier,
                               final int batchSize) {
        log.info(
                "Failed to sync required number of processes (id = {}, number = {}) using master lock with id = {}",
                processIdentifier, batchSize, masterId
        );
        updateMasterLock(masterId, processIdentifier, SyncStatus.FAILED);
    }

    private SyncResult attemptToSynchronize(final long masterId,
                                            final String processIdentifier,
                                            final int batchSize,
                                            final HashMap<Long, Integer> failedAttemptsBySlaveId) {
        final var availableSlaveIds = getAvailableSlaveIds(masterId, processIdentifier);
        if (availableSlaveIds.size() >= batchSize) {
            final var slavesToProcess = new ArrayList<Long>();
            availableSlaveIds.stream()
                    .filter(slaveId -> {
                        final var failedAttempts = failedAttemptsBySlaveId.getOrDefault(slaveId, 0);
                        return failedAttempts <= FAILED_ATTEMPTS_THRESHOLD;
                    })
                    .takeWhile(ignored -> slavesToProcess.size() < batchSize)
                    .forEach(slaveId -> {
                        if (!Objects.equals(masterId, slaveId) && canAcquireLockOnSlave(slaveId)) {
                            final var failedAttempts = failedAttemptsBySlaveId.getOrDefault(slaveId, 0);
                            failedAttemptsBySlaveId.put(slaveId, failedAttempts + 1);
                        } else {
                            slavesToProcess.add(slaveId);
                        }
                    });

            if (slavesToProcess.size() == batchSize) {
                onSyncSuccess(masterId, processIdentifier, batchSize, slavesToProcess);
                return SyncResult.SUCCESS;
            }
        }

        log.info("""
                        Processing stats:
                        - slaves: {},\s
                        - attempts per slave: {}""",
                availableSlaveIds.stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(",")),
                failedAttemptsBySlaveId.entrySet().stream()
                        .map(entry -> "(id: %s, failed attempts: %s)".formatted(entry.getKey(), entry.getValue()))
                        .collect(Collectors.joining(","))
        );
        return SyncResult.REPEAT_REQUIRED;
    }

    private void onSyncSuccess(final long masterId,
                               final String processIdentifier,
                               final int batchSize,
                               final List<Long> slavesToProcess) {
        log.info(
                "Master (id = {}) was able to sync requested number of " +
                        "the slave processors ({}, including itself) for the process with id {}",
                masterId, batchSize, processIdentifier
        );
        updateMasterLock(masterId, processIdentifier, SyncStatus.SYNCED);
        slavesToProcess.forEach(slaveId -> createProcessingRecords(masterId, slaveId));
    }

    private List<Long> getAvailableSlaveIds(final Long masterId, final String processIdentifier) {
        log.info("Retrieving a list of available slaves for the process with identifier = {}", processIdentifier);
        final var slaves = jdbcTemplate.queryForStream(
                """
                        select s.id
                        from db_slave_lock s
                                 left join db_slave_per_master spm on s.id = spm.slave_id
                                 left join db_master_lock m on m.id = spm.master_id
                        where s.process_identifier = ?
                          and (m.status is null or m.status != 'SYNCED')
                         """,
                ps -> ps.setString(1, processIdentifier),
                (rs, rowNum) -> rs.getLong(1)
        ).collect(Collectors.toList());
        slaves.add(0, masterId);
        log.info(
                "Found following slaves (including master): [{}]",
                slaves.stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(","))
        );
        return slaves;
    }

    private boolean canAcquireLockOnSlave(final long slaveId) {
        log.info("Attempting to acquire lock on the slave (id = {})", slaveId);
        try {
            return transactionalProcessor.execute(
                    () -> jdbcTemplate.queryForStream(
                            "select * from db_slave_lock where id = ? for update",
                            ps -> {
                                ps.setQueryTimeout(LOCK_OBTAINING_TIMEOUT);
                                ps.setLong(1, slaveId);
                            },
                            (rs, rowNum) -> true
                    ).findFirst().isPresent()
            );
        } catch (final DataAccessException exception) {
            if (isQueryTimeoutException(exception)) {
                return false;
            }
            throw exception;
        }
    }

    private void createProcessingRecords(final Long masterId, final Long slaveId) {
        if (Objects.equals(masterId, slaveId)) return;
        jdbcTemplate.update(
                "insert into db_slave_per_master(master_id, slave_id) values (?, ?)",
                ps -> {
                    ps.setLong(1, masterId);
                    ps.setLong(2, slaveId);
                }
        );
    }

    private void updateMasterLock(final long masterId,
                                  final String processIdentifier,
                                  final SyncStatus status) {
        jdbcTemplate.update(
                "update db_master_lock set id = ?, status = ? where process_identifier = ? and id is null",
                ps -> {
                    ps.setLong(1, masterId);
                    ps.setString(2, status.toString());
                    ps.setString(3, processIdentifier);
                }
        );
    }
}
