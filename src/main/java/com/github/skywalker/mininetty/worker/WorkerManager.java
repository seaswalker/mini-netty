package com.github.skywalker.mininetty.worker;

import com.github.skywalker.mininetty.manager.AbstractManager;
import com.github.skywalker.mininetty.manager.ChooseStrategy;

/**
 * Manages the worker threads.
 * 
 * @author skywalker
 *
 */
public class WorkerManager extends AbstractManager<Worker> {

    public WorkerManager(int s) {
        super(s);
    }

    public WorkerManager(int s, ChooseStrategy<Worker> chooseStrategy) {
        super(s, chooseStrategy);
    }

    @Override
    protected Worker newCandidate() {
        return new Worker();
    }

}
