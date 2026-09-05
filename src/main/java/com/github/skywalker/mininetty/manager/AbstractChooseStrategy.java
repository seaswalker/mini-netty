package com.github.skywalker.mininetty.manager;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Provides skeleton functionality for {@link ChooseStrategy}.
 *
 * @author skywalker
 */
public abstract class AbstractChooseStrategy<T> implements ChooseStrategy<T> {

    protected List<T> candidates;
    protected int index = 0;
    protected int length;

    private final Lock lock = new ReentrantLock();

    @Override
    public final T choose() {
        lock.lock();
        T result;
        try {
            result = doChoose();
            return result;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void setCandidates(List<T> candidates) {
        this.candidates = candidates;
        this.length = candidates.size();
    }

    /**
     * Performs the actual selection; subclasses may override this.
     *
     * @return the chosen candidate
     */
    public T doChoose() {
        T result = candidates.get(index);
        incIndex();
        return result;
    }

    private void incIndex() {
        ++index;
        if (index >= length)
            index = 0;
    }

}
