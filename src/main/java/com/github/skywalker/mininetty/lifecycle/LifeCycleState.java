package com.github.skywalker.mininetty.lifecycle;

import com.github.skywalker.mininetty.exception.MiniNettyIllegalStateException;

public class LifeCycleState {

    private LifeCycle.State state = LifeCycle.State.INIT;

    public boolean start() {
        if (state == LifeCycle.State.INIT) {
            state = LifeCycle.State.STARTED;
            return true;
        }
        return false;
    }

    public boolean close() {
        if (state == LifeCycle.State.CLOSED) {
            return false;
        }
        state = LifeCycle.State.CLOSED;
        return true;
    }

    public void assertStarted() {
        if (state != LifeCycle.State.STARTED) {
            throw new MiniNettyIllegalStateException("Unstarted");
        }
    }

}
