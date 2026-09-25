package com.github.lisama569.distributedcoordinationservicejava.core;

/**
 * 携带明确 {@link CoordinationErrorCode} 的协调异常，
 * 便于上层（测试/REST 层）按失败原因做可区分断言。
 */
public class CoordinationException extends RuntimeException {

    private final CoordinationErrorCode errorCode;

    public CoordinationException(CoordinationErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public CoordinationErrorCode getErrorCode() {
        return errorCode;
    }
}
