package com.github.lisama569.distributedcoordinationservicejava.cluster;

/**
 * 日志复制响应。success=false 且 term 更大时，领导者必须下台。
 *
 * @param term         对端当前任期
 * @param success      prevLog 是否匹配且本批日志处理成功
 * @param matchIndex   成功时对端已匹配到位的日志位点
 * @param lastLogIndex 失败时对端保留窗口的信息：其本地最后日志位点；
 *                     领导者据此判断该节点是“已被压缩、需要发存档”还是“仅需回退 nextIndex”
 */
public record AppendResponse(long term, boolean success, long matchIndex, long lastLogIndex) {

    public static AppendResponse ok(long term, long matchIndex) {
        return new AppendResponse(term, true, matchIndex, matchIndex);
    }

    public static AppendResponse mismatch(long term, long peerLastLogIndex) {
        return new AppendResponse(term, false, 0L, peerLastLogIndex);
    }
}
