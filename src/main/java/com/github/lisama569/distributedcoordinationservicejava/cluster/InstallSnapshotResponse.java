package com.github.lisama569.distributedcoordinationservicejava.cluster;

/**
 * InstallSnapshot 响应。
 *
 * @param term       对端当前任期（更大时领导者必须下台）
 * @param success    是否安装成功
 * @param matchIndex 安装成功后对端已匹配到位的日志位点
 */
public record InstallSnapshotResponse(long term, boolean success, long matchIndex) {
}
