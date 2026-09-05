package com.github.skywalker.mininetty.manager;

import com.github.skywalker.mininetty.exception.MiniNettyIllegalStateException;
import com.github.skywalker.mininetty.lifecycle.LifeCycle;
import com.github.skywalker.mininetty.lifecycle.LifeCycleState;
import com.github.skywalker.mininetty.selector.SelectorManager;
import com.github.skywalker.mininetty.worker.WorkerManager;

public class ManagerGroup implements LifeCycle {

    private final SelectorManager selectorManager;
    private final WorkerManager workerManager;
    private final LifeCycleState lifeCycleState = new LifeCycleState();

    private static final int defaultSelectorCount = 1;
    private static final int defaultWorkerCount = Runtime.getRuntime().availableProcessors();

    public ManagerGroup() {
        this(defaultSelectorCount, defaultWorkerCount);
    }

    public ManagerGroup(int selectorCount, int workerCount) {
        if (selectorCount < 1 || workerCount < 1) {
            throw new MiniNettyIllegalStateException("Selector count and worker count must be greater than 0");
        }
        this.selectorManager = new SelectorManager(selectorCount);
        this.workerManager = new WorkerManager(workerCount);
        this.selectorManager.setWorkerManager(workerManager);
    }

    public SelectorManager getSelectorManager() {
        lifeCycleState.assertStarted();
        return selectorManager;
    }

    @Override
    public void start() {
        if (lifeCycleState.start()) {
            this.selectorManager.start();
            this.workerManager.start();
        }
    }

    @Override
    public void close() {
        if (lifeCycleState.close()) {
            this.selectorManager.close();
            this.workerManager.close();
        }
    }
}
