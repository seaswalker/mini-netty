package com.github.skywalker.mininetty.manager;

import java.util.List;

/**
 * Strategy for choosing a thread.
 *
 * @author skywalker
 */
public interface ChooseStrategy<T> {

    T choose(Object param);

    T choose(Object param, PostAction postAction);

    void setCandidates(List<T> candidates);

    enum PostAction {
        BIND,
        UNBIND,
        NOOP
    }

}
