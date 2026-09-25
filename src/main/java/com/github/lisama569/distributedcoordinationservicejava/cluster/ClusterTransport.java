package com.github.lisama569.distributedcoordinationservicejava.cluster;

import java.util.Collection;
import java.util.Optional;

/**
 * 集群传输抽象：由进程内的 ClusterSimulator 实现。
 * 分区时 {@link #sendRequestVote}/{@link #sendAppendEntries} 返回 empty，
 * 节点据此感知“对端不可达/超时”。
 */
public interface ClusterTransport {

    Collection<String> nodeIds();

    /** 从 from 向 to 发送拉票请求；被分区隔离时返回 empty。 */
    Optional<VoteResponse> sendRequestVote(String from, String to, VoteRequest request);

    /** 从 from 向 to 发送复制/心跳请求；被分区隔离时返回 empty。 */
    Optional<AppendResponse> sendAppendEntries(String from, String to, AppendRequest request);
}
