package com.shvatov.dblocks.utils;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.ResultSet;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class JdbcTemplateUtils {
    private JdbcTemplateUtils() {
    }

    public static <L> Optional<L> querySingleWithTimeout(final JdbcOperations jdbcTemplate,
                                                         final String sql,
                                                         final Function<ResultSet, L> mapper,
                                                         final Integer timeout) {
        return JdbcTemplateUtils.queryWithTimeout(jdbcTemplate, sql, mapper, timeout)
                .findFirst();
    }

    public static <L> Stream<L> queryWithTimeout(final JdbcOperations jdbcTemplate,
                                                 final String sql,
                                                 final Function<ResultSet, L> mapper,
                                                 final Integer timeout) {
        return JdbcTemplateUtils.queryWithTimeout(
                jdbcTemplate,
                () -> jdbcTemplate.queryForStream(sql, (rs, rowNum) -> mapper.apply(rs)),
                timeout
        );
    }

    public static <T> T queryWithTimeout(final JdbcOperations jdbcTemplate,
                                         final Supplier<T> query,
                                         final Integer timeout) {
        if (Objects.nonNull(timeout) && jdbcTemplate instanceof final JdbcTemplate template) {
            final var overriddenTimeout = template.getQueryTimeout();
            template.setQueryTimeout(timeout);

            final T result;
            try {
                result = query.get();
            } finally {
                template.setQueryTimeout(overriddenTimeout);
            }
            return result;
        } else {
            return query.get();
        }
    }
}
