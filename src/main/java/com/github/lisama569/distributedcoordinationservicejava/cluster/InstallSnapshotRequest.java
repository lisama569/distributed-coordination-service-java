package com.github.lisama569.distributedcoordinationservicejava.cluster;

import com.github.lisama569.distributedcoordinationservicejava.recovery.Snapshot;

/**
 * InstallSnapshot RPC：落后节点的所需历史已被领导者压缩时，
 * 领导者直接发送完整状态存档，对端先安装存档再补增量。
 */
public record InstallSnapshotRequest(long term, String leaderId, Snapshot snapshot) {
}
