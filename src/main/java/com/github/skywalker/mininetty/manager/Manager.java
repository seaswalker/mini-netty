package com.github.skywalker.mininetty.manager;

import com.github.skywalker.mininetty.lifecycle.LifeCycle;

/**
 * Manages a pool of threads (or thread-like components).
 *
 * @author skywalker
 */
public interface Manager<T> extends LifeCycle {

    /**
     * Picks one of the managed threads.
     */
    T chooseOne(Object param);

    T chooseOne(Object param, ChooseStrategy.PostAction postAction);

}
