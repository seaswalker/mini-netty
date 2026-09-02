package com.github.skywalker.mininetty.selector;

import com.github.skywalker.mininetty.worker.WorkerManager;
import com.github.skywalker.mininetty.manager.AbstractManager;
import com.github.skywalker.mininetty.manager.ChooseStrategy;

/**
 * Manages the selectors: starts them and load-balances client connections across them.
 *
 * @author skywalker
 */
public class SelectorManager extends AbstractManager<QueuedSelector> {

    private WorkerManager workerManager;

    public SelectorManager(int s) {
        super(s);
    }

    public SelectorManager(int s, ChooseStrategy<QueuedSelector> chooseStrategy) {
        super(s, chooseStrategy);
    }

    public void setWorkerManager(WorkerManager workerManager) {
        this.workerManager = workerManager;
    }

    @Override
    protected QueuedSelector newCandidate() {
        QueuedSelector selector = new QueuedSelector();
        selector.setWorkerManager(workerManager);
        return selector;
    }

}
