package com.github.lisama569.distributedcoordinationservicejava.cluster;

import java.util.Collection;
import java.util.Optional;

/**
 * 集群传输抽象：由进程内的 ClusterSimulator 实现。
 * 分区时 {@link #sendRequestVote}/{@link #sendAppendEntries}/{@link #sendInstallSnapshot}
 * 返回 empty，节点据此感知“对端不可达/超时”。
 */
public interface ClusterTransport {

    Collection<String> nodeIds();

    /**
     * 节点的当前"成员代次"：每次显式重置重加入（清空本地历史）都会单调 +1。
     * 领导者据此识别"同名但磁盘已清空"的对端，丢弃其旧的复制进度并从存档重新同步。
     */
    long incarnationOf(String nodeId);

    /** 从 from 向 to 发送拉票请求；被分区隔离时返回 empty。 */
    Optional<VoteResponse> sendRequestVote(String from, String to, VoteRequest request);

    /** 从 from 向 to 发送复制/心跳请求；被分区隔离时返回 empty。 */
    Optional<AppendResponse> sendAppendEntries(String from, String to, AppendRequest request);

    /** 从 from 向 to 发送安装存档请求（追赶落后节点）；被分区隔离时返回 empty。 */
    Optional<InstallSnapshotResponse> sendInstallSnapshot(String from, String to,
                                                          InstallSnapshotRequest request);
}
