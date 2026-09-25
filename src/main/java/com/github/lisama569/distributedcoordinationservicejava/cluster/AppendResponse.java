package com.github.lisama569.distributedcoordinationservicejava.cluster;

/**
 * 日志复制响应。success=false 且 term 更大时，领导者必须下台。
 */
public record AppendResponse(long term, boolean success, long matchIndex) {
}
