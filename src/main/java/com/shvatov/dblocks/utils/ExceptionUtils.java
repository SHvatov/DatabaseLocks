package com.shvatov.dblocks.utils;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

public class ExceptionUtils {
    public static record ExecutionResult<V>(Optional<V> result, Optional<Throwable> exception) {
        public ExecutionResult {
            if (result.isPresent() && exception.isPresent()
                    || result.isEmpty() && exception.isEmpty()) {
                throw new IllegalArgumentException("Either result or exception must be present!");
            }
        }

        public <R> ExecutionResult<R> map(final Function<V, R> mapper) {
            return result.map(it -> runCatching(() -> mapper.apply(it)))
                    .orElse(new ExecutionResult<>(Optional.empty(), this.exception));
        }

        public ExecutionResult<V> onSuccess(final Consumer<V> consumer) {
            result.ifPresent(consumer);
            return this;
        }

        public ExecutionResult<V> onError(final Consumer<Throwable> consumer) {
            exception.ifPresent(consumer);
            return this;
        }

        public V getResultOrThrow() {
            return result.orElseThrow();
        }
    }

    public static <V> ExecutionResult<V> runCatching(final Callable<V> block) {
        try {
            final var result = block.call();
            return new ExecutionResult<>(Optional.of(result), Optional.empty());
        } catch (final Exception exception) {
            return new ExecutionResult<>(Optional.empty(), Optional.of(exception));
        }
    }
}
