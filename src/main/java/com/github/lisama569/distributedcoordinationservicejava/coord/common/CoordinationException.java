package com.github.lisama569.distributedcoordinationservicejava.coord.common;

/** 协调服务对外抛出的统一异常，携带可区分的 {@link RejectReason}。 */
public class CoordinationException extends RuntimeException {

    private final RejectReason reason;

    public CoordinationException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason getReason() {
        return reason;
    }
}
