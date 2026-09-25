package com.github.lisama569.distributedcoordinationservicejava.lock;

/**
 * 栅栏令牌：随领导者任期单调递增，写入时携带，用于拒绝旧持有者的迟到写。
 *
 * @param term     颁发令牌时的领导者任期
 * @param seq      同一任期内的单调序号（保证令牌全局严格递增）
 * @param holderId 锁持有者标识
 * @param lockName 锁名称
 */
public record FenceToken(long term, long seq, String holderId, String lockName)
        implements Comparable<FenceToken> {

    /**
     * 令牌序：先比任期，再比序号。令牌只能前进不能回退。
     */
    @Override
    public int compareTo(FenceToken other) {
        int byTerm = Long.compare(this.term, other.term);
        if (byTerm != 0) {
            return byTerm;
        }
        return Long.compare(this.seq, other.seq);
    }

    /**
     * 全局排序键：任期在高位、任期内序号在低位。
     * 任期/序号均为正数，且单任期内序号远小于 {@link Long#MAX_VALUE} 的可分配范围。
     */
    public long orderingKey() {
        return term * 1_000_000L + seq;
    }

    @Override
    public String toString() {
        return "FenceToken(term=" + term + ", seq=" + seq
                + ", holder=" + holderId + ", lock=" + lockName + ")";
    }
}
