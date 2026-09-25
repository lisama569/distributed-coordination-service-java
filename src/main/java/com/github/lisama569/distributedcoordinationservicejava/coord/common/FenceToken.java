package com.github.lisama569.distributedcoordinationservicejava.coord.common;

/**
 * 栅栏令牌：高位为任期、低位为任期内单调递增序号，整体随任期单调递增。
 * 新版本令牌永远大于旧版本令牌，迟到的旧持有者写操作会被拒绝。
 *
 * <p>编码布局：{@code encoded = (term << SEQUENCE_BITS) | sequence}，
 * 同一任期内序号递增，任期一旦变大，编码值必然大于旧任期的任何令牌。</p>
 */
public record FenceToken(long term, long sequence) implements Comparable<FenceToken> {

    public static final long SEQUENCE_BITS = 20;
    private static final long SEQUENCE_MASK = (1L << SEQUENCE_BITS) - 1;
    private static final long MAX_SEQUENCE = SEQUENCE_MASK;

    public FenceToken {
        if (term <= 0) {
            throw new IllegalArgumentException("栅栏令牌任期必须为正数: " + term);
        }
        if (sequence < 0 || sequence > MAX_SEQUENCE) {
            throw new IllegalArgumentException("栅栏令牌序号越界: " + sequence);
        }
    }

    public long encoded() {
        return (term << SEQUENCE_BITS) | sequence;
    }

    public static FenceToken decode(long encoded) {
        if (encoded <= 0) {
            throw new IllegalArgumentException("非法栅栏令牌编码: " + encoded);
        }
        return new FenceToken(encoded >>> SEQUENCE_BITS, encoded & SEQUENCE_MASK);
    }

    @Override
    public int compareTo(FenceToken other) {
        int byTerm = Long.compare(this.term, other.term);
        if (byTerm != 0) {
            return byTerm;
        }
        return Long.compare(this.sequence, other.sequence);
    }

    @Override
    public String toString() {
        return "FenceToken(term=" + term + ",seq=" + sequence + ",encoded=" + encoded() + ")";
    }
}
