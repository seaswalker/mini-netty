package com.github.skywalker.mininetty.lifecycle;

import org.jspecify.annotations.Nullable;

import java.util.concurrent.Future;

/**
 * The lifecycle of a component.
 *
 * @author skywalker
 */
public interface LifeCycle {

    /**
     * Starts the component.
     */
    void start();

    void close();

    default @Nullable Future<?> startAsync() {
        throw new UnsupportedOperationException();
    }

    enum State {
        INIT,
        STARTED,
        CLOSED
    }

}
