package com.github.skywalker.mininetty.server;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.github.skywalker.mininetty.exception.MiniNettyIllegalStateException;
import com.github.skywalker.mininetty.lifecycle.LifeCycleState;
import com.github.skywalker.mininetty.manager.ManagerGroup;

import com.github.skywalker.mininetty.handler.Handler;
import com.github.skywalker.mininetty.lifecycle.LifeCycle;

/**
 * Server.
 *
 * @author skywalker
 */
public final class Server implements LifeCycle {

    private final List<Handler> handlers = new LinkedList<>();
    private final ManagerGroup managerGroup;
    private final LifeCycleState lifeCycleState = new LifeCycleState();

    private int port = -1;
    private Future<?> future;

    public Server(ManagerGroup managerGroup) {
        if (managerGroup == null) {
            throw new MiniNettyIllegalStateException("ManagerGroup is null");
        }
        this.managerGroup = managerGroup;
    }

    /**
     * Binds this server to the given port.
     */
    public Server bind(int port) {
        if (port < 1)
            throw new MiniNettyIllegalStateException(
                    "The port must be greater than 0.");
        this.port = port;
        return this;
    }

    public Server setHandlers(Handler... handlers) {
        if (handlers.length < 1) {
            throw new MiniNettyIllegalStateException("No handlers specified");
        }
        this.handlers.addAll(Arrays.asList(handlers));
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

    /**
     * Starts the server.
     */
    @Override
    public Future<?> startAsync() {
        checkRequires();
        if (lifeCycleState.start()) {
            internalStartAsync();
        }
        return future;
    }

    @Override
    public void close() {
        if (lifeCycleState.close()) {
            if (future != null) {
                future.cancel(true);
            }
        }
    }

    private void checkRequires() {
        if (handlers.isEmpty()) {
            throw new MiniNettyIllegalStateException("No handlers specified");
        }
        if (port < 0) {
            throw new MiniNettyIllegalStateException("No port bound?");
        }
    }

    private void internalStartAsync() {
        this.future = managerGroup.getSelectorManager().chooseOne().registerServerChannelAsync(port, handlers);
    }

}
