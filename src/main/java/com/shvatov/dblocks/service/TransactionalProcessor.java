package com.shvatov.dblocks.service;

import lombok.SneakyThrows;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.Callable;

@Service
public class TransactionalProcessor {
    @SneakyThrows
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public <R> R execute(final Callable<R> action) {
        return action.call();
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void process(final Runnable action) {
        action.run();
    }
}
