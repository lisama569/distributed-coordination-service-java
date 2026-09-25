package com.github.lisama569.distributedcoordinationservicejava.coord.statemachine;

import com.github.lisama569.distributedcoordinationservicejava.coord.common.FenceToken;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.RejectReason;

/**
 * 状态机应用一条日志后的结论。{@code accepted=false} 时携带可区分的 {@link RejectReason}，
 * 拒绝同样会被提交并记入幂等表，重试得到同一结论。
 */
public record Outcome(boolean accepted, RejectReason reason, FenceToken fenceToken,
                      String detail, String kvValue) {

    public static Outcome ok(FenceToken token, String detail) {
        return new Outcome(true, null, token, detail, null);
    }

    public static Outcome reject(RejectReason reason, FenceToken token, String detail) {
        return new Outcome(false, reason, token, detail, null);
    }
}
