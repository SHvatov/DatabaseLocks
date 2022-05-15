package com.shvatov.dblocks.model.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum LockMode {
    EXCLUSIVE("update", "pg_advisory_xact_lock"),
    SHARED("share", "pg_advisory_xact_lock_shared");

    private final String sqlKeyWord;
    private final String pgLockFunction;
}
