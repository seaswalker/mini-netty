package com.github.skywalker.mininetty.lifecycle;

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

}
