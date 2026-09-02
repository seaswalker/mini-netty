package com.github.skywalker.mininetty.manager;

import java.util.ArrayList;
import java.util.List;

import com.github.skywalker.mininetty.lifecycle.LifeCycle;

/**
 * A skeletal {@link Manager} implementation.
 *
 * @author skywalker
 */
public abstract class AbstractManager<T extends LifeCycle> implements Manager<T> {

    private final List<T> candidates;
    private final ChooseStrategy<T> chooseStrategy;
    private final int s;

    public AbstractManager(int s) {
        this(s, null);
    }

    public AbstractManager(int s, ChooseStrategy<T> chooseStrategy) {
        if (s < 1) {
            throw new IllegalArgumentException("The candidates count can't be less than 1.");
        }
        candidates = new ArrayList<>(s);
        this.s = s;
        if (chooseStrategy != null) {
            this.chooseStrategy = chooseStrategy;
        } else {
            this.chooseStrategy = new DefaultRoundRobinStrategy<>();
        }
    }

    @Override
    public void start() {
        for (int i = 0; i < s; i++) {
            T candidate = newCandidate();
            candidates.add(candidate);
            candidate.start();
        }
        chooseStrategy.setCandidates(candidates);
    }

    @Override
    public void close() {
        candidates.forEach(LifeCycle::close);
    }

    /**
     * Creates a new candidate.
     *
     * @return a new candidate
     */
    protected abstract T newCandidate();

    @Override
    public T chooseOne(Object param) {
        return chooseStrategy.choose(param);
    }

    @Override
    public T chooseOne(Object param, ChooseStrategy.PostAction postAction) {
        return chooseStrategy.choose(param, postAction);
    }
}
