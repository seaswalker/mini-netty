package com.github.skywalker.mininetty.client;

import com.github.skywalker.mininetty.exception.MiniNettyIllegalStateException;
import com.github.skywalker.mininetty.handler.Handler;
import com.github.skywalker.mininetty.lifecycle.LifeCycle;
import com.github.skywalker.mininetty.lifecycle.LifeCycleState;
import com.github.skywalker.mininetty.manager.ManagerGroup;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Client implements LifeCycle {

    private final String host;
    private final int port;
    private final LifeCycleState lifeCycleState = new LifeCycleState();

    private ManagerGroup managerGroup;
    private List<Handler> handlers;
    private Future<?> future;

    public Client(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public Client setManagerGroup(ManagerGroup managerGroup) {
        this.managerGroup = managerGroup;
        return this;
    }

    public Client handlers(Handler...handlers) {
        this.handlers = Arrays.asList(handlers);
        return this;
    }

    @Override
    public void start() {
        checkRequires();
        if (lifeCycleState.start()) {
            internalStartAsync();
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new MiniNettyIllegalStateException(e);
            }
        }
    }

    @Override
    public void close() {
        if (lifeCycleState.close()) {
            if (future != null) {
                future.cancel(true);
            }
        }
    }

    @Override
    public Future<?> startAsync() {
        checkRequires();
        if (lifeCycleState.start()) {
            internalStartAsync();
        }
        return future;
    }

    private void internalStartAsync() {
        future = managerGroup.getSelectorManager().chooseOne().registerClientChannelAsync(host, port, handlers);
    }

    private void checkRequires() {
        if (managerGroup == null) {
            throw new IllegalArgumentException("ManagerGroup not set");
        }
        if (handlers == null || handlers.isEmpty()) {
            throw new IllegalArgumentException("Handlers not set");
        }
    }

}
