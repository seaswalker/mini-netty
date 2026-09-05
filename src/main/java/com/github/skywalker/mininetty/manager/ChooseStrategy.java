package com.github.skywalker.mininetty.manager;

import java.util.List;

/**
 * Strategy for choosing a thread.
 *
 * @author skywalker
 */
public interface ChooseStrategy<T> {

    T choose();

    void setCandidates(List<T> candidates);

}
