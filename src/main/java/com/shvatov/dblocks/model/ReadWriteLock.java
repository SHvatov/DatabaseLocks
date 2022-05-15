package com.shvatov.dblocks.model;

import com.shvatov.dblocks.model.enums.LockMode;

public record ReadWriteLock(String processIdentifier, LockMode mode) {
    public static final String TABLE_NAME = "db_lock";
    public static final String PROCESS_IDENTIFIER_COLUMN_NAME = "process_identifier";
}
