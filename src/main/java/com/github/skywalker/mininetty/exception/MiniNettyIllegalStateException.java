package com.github.skywalker.mininetty.exception;

public class MiniNettyIllegalStateException extends RuntimeException {

    public MiniNettyIllegalStateException(String message) {
        super(message);
    }

    public MiniNettyIllegalStateException(Exception e) {
        super(e);
    }

    public MiniNettyIllegalStateException(String message, Exception e) {
        super(message, e);
    }

}
