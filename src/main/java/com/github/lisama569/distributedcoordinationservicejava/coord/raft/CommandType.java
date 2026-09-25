package com.github.lisama569.distributedcoordinationservicejava.coord.raft;

/** 复制状态机支持的命令类型。 */
public enum CommandType {
    LOCK_ACQUIRE,
    LOCK_RENEW,
    LOCK_RELEASE,
    KV_WRITE
}
