package com.github.lisama569.distributedcoordinationservicejava.cluster;

import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationErrorCode;

/**
 * 安装存档 RPC 的响应。
 *
 * @param term       接收方当前任期（大于请求任期时领导者必须下台）
 * @param success    是否安装成功
 * @param snapIndex  接收方安装/拒绝时的存档位点，便于领导者判定进度
 * @param reason     拒绝原因；典型为 {@link CoordinationErrorCode#OUT_OF_RETENTION_WINDOW}
 *                   （自带旧历史且超出可恢复窗口，必须重置后重加入）
 */
public record InstallSnapshotResponse(long term, boolean success, long snapIndex, String reason) {

    public static InstallSnapshotResponse ok(long term, long snapIndex) {
        return new InstallSnapshotResponse(term, true, snapIndex, null);
    }

    public static InstallSnapshotResponse reject(long term, long snapIndex, String reason) {
        return new InstallSnapshotResponse(term, false, snapIndex, reason);
    }
}
