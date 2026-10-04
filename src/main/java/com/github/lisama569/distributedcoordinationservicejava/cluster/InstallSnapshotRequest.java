package com.github.lisama569.distributedcoordinationservicejava.cluster;

import com.github.lisama569.distributedcoordinationservicejava.snapshot.Snapshot;

/**
 * 安装存档 RPC（InstallSnapshot）：领导者把自己的历史存档整体推给落后对端。
 *
 * @param term     领导者任期
 * @param leaderId 领导者节点 id
 * @param snapshot 历史存档（覆盖 1..lastIncludedIndex 的已提交应用状态）
 */
public record InstallSnapshotRequest(long term, String leaderId, Snapshot snapshot) {
}
