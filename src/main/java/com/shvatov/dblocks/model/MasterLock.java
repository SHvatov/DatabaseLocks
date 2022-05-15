package com.shvatov.dblocks.model;

import com.shvatov.dblocks.model.enums.SyncStatus;

public record MasterLock(Long id, String processIdentifier, SyncStatus status) {}
