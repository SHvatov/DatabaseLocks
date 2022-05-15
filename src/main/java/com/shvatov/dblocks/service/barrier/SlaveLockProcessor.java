package com.shvatov.dblocks.service.barrier;

import com.shvatov.dblocks.model.MasterLock;
import com.shvatov.dblocks.model.SlaveLock;
import com.shvatov.dblocks.model.enums.SyncResult;
import com.shvatov.dblocks.model.enums.SyncStatus;
import com.shvatov.dblocks.service.TransactionalProcessor;
import com.shvatov.dblocks.service.seq.SequenceValueGenerator;
import com.shvatov.dblocks.utils.ExceptionUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class SlaveLockProcessor {
    private final SequenceValueGenerator sequenceValueGenerator;
    private final TransactionalProcessor transactionalProcessor;
    private final JdbcTemplate jdbcTemplate;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public SyncResult process(final String processIdentifier) {
        log.info("Started the processing of a new slave for the process with identifier = {}", processIdentifier);

        final var slaveId = sequenceValueGenerator.nextValue();
        transactionalProcessor.process(() -> createSlaveLock(processIdentifier, slaveId));

        final var slaveLock = acquireSlaveLock(slaveId);
        transactionalProcessor.execute(() -> acquireMasterLock(processIdentifier))
                .ifPresent(this::updateFailedMasterLockAndThrow);

        final var masterLockOpt = getMasterDataAfterProcessing(slaveLock);
        final var hasParticipated = masterLockOpt.isPresent();
        if (!hasParticipated) {
            deleteSlaveLock(slaveLock);
            return SyncResult.REPEAT_REQUIRED;
        }

        final var masterLock = masterLockOpt.get();
        final var isSyncSuccess = Objects.equals(SyncStatus.SYNCED, masterLock.status());
        if (isSyncSuccess) {
            return SyncResult.SUCCESS;
        }
        return SyncResult.FAILURE;
    }

    private void deleteSlaveLock(final SlaveLock slaveLock) {
        log.info(
                "Deleting not used slave (id = {}) for the process with identifier = {}",
                slaveLock.id(), slaveLock.processIdentifier()
        );
        jdbcTemplate.update(
                "delete from db_slave_lock where id = ?",
                ps -> ps.setLong(1, slaveLock.id())
        );
    }

    private void createSlaveLock(final String processIdentifier, final long slaveId) {
        log.info("Creating a new slave (id = {}) for the process with identifier = {}", slaveId, processIdentifier);
        jdbcTemplate.update(
                "insert into db_slave_lock(id, process_identifier) values(?, ?)",
                ps -> {
                    ps.setLong(1, slaveId);
                    ps.setString(2, processIdentifier);
                }
        );
    }

    private SlaveLock acquireSlaveLock(final long slaveId) {
        log.info("Attempting to acquire lock on the slave (id = {})", slaveId);
        return jdbcTemplate.queryForStream(
                "select * from db_slave_lock where id = ? for update",
                ps -> ps.setLong(1, slaveId),
                (rs, rowNum) -> ExceptionUtils.runCatching(
                        () -> new SlaveLock(
                                rs.getLong("id"),
                                rs.getString("process_identifier")
                        )
                ).getResultOrThrow()
        ).findFirst().orElseThrow(
                () -> new IllegalStateException(
                        "Could not acquire slave lock with id = %s"
                                .formatted(slaveId)
                )
        );
    }

    private Optional<MasterLock> acquireMasterLock(final String processIdentifier) {
        log.info("Awaiting the lock on the master for the process with identifier = {}", processIdentifier);
        return jdbcTemplate.queryForStream(
                "select * from db_master_lock where process_identifier = ? and id is null for update",
                ps -> ps.setString(1, processIdentifier),
                (rs, rowNum) -> ExceptionUtils.runCatching(
                        () -> new MasterLock(
                                rs.getLong("id"),
                                rs.getString("process_identifier"),
                                SyncStatus.valueOf(rs.getString("status"))
                        )
                ).getResultOrThrow()
        ).findFirst();
    }

    private Optional<MasterLock> getMasterDataAfterProcessing(final SlaveLock slaveLock) {
        log.info(
                "Checking whether sync of the process with identifier = {} has succeeded",
                slaveLock.processIdentifier()
        );

        return jdbcTemplate.queryForStream(
                """
                        select m.id as master_id, m.status as status from db_slave_per_master ms
                            join db_master_lock m on m.id = ms.master_id
                            where ms.slave_id = ?""",
                ps -> ps.setLong(1, slaveLock.id()),
                (rs, rowNum) -> ExceptionUtils.runCatching(
                        () -> new MasterLock(
                                rs.getLong("master_id"),
                                slaveLock.processIdentifier(),
                                SyncStatus.valueOf(rs.getString("status"))
                        )
                ).getResultOrThrow()
        ).findFirst();
    }

    private void updateFailedMasterLockAndThrow(final MasterLock masterLock) {
        final var newMasterId = sequenceValueGenerator.nextValue();
        log.error(
                "Unexpectedly acquired lock on the master ({}). New id ({}) and status({}) will be set.",
                masterLock, newMasterId, SyncStatus.FAILED
        );
        transactionalProcessor.process(() -> updateMasterLockStatus(masterLock, newMasterId));
        throw new IllegalStateException("Unexpectedly acquired master lock!");
    }

    private void updateMasterLockStatus(final MasterLock masterLock, final long newId) {
        jdbcTemplate.update(
                "update db_master_lock set id = ?, status = ? where process_identifier = ? and id is null",
                ps -> {
                    ps.setLong(1, newId);
                    ps.setString(2, SyncStatus.FAILED.toString());
                    ps.setString(3, masterLock.processIdentifier());
                }
        );
    }
}
