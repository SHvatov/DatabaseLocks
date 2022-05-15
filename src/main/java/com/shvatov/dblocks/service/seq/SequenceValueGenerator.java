package com.shvatov.dblocks.service.seq;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.util.Objects;

@Service
@RequiredArgsConstructor
public class SequenceValueGenerator {
    private static final String ID_SEQ_NAME = "seq_lock_id";

    private final JdbcTemplate jdbcTemplate;

    public long nextValue() {
        return Objects.requireNonNull(
                jdbcTemplate.query(
                        "select nextval('%s') as id".formatted(ID_SEQ_NAME),
                        (ResultSet rs, int rowNum) -> rs.getLong("id")
                ).get(0)
        );
    }
}
