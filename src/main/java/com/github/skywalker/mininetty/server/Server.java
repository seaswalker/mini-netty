package com.github.skywalker.mininetty.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.github.skywalker.mininetty.manager.DefaultRoundRobinStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.skywalker.mininetty.handler.Handler;
import com.github.skywalker.mininetty.lifecycle.LifeCycle;
import com.github.skywalker.mininetty.selector.SelectorManager;
import com.github.skywalker.mininetty.util.CloseableUtils;
import com.github.skywalker.mininetty.worker.WorkerManager;

/**
 * Server.
 *
 * @author skywalker
 */
public final class Server implements LifeCycle {

    // Listen in blocking mode by default
    private boolean block = true;
    private ServerSocketChannel serverSocketChannel;

    private volatile boolean closed = false;

    private final List<Handler> handlers = new LinkedList<>();
    private final ExecutorService executor;
    private final SelectorManager selectorManager;
    private final WorkerManager workerManager;
    private final int acceptors;

    private final static Logger logger = LoggerFactory.getLogger(Server.class);

    public Server() {
        this(0, 0);
    }

    public Server(int acceptors, int workers) {
        int cores = Runtime.getRuntime().availableProcessors();
        int max = Math.max(2, Math.min(4, cores / 8));
        if (acceptors <= 1) {
            acceptors = max;
        }
        if (workers <= 0) {
            workers = max;
        }
        if (acceptors > cores) {
            throw new IllegalArgumentException(
                    "The acceptors count must not exceed the number of cores.");
        }
        if (workers > cores) {
            throw new IllegalArgumentException(
                    "The workers count must not exceed the number of cores.");
        }
        executor = Executors.newFixedThreadPool(acceptors + workers);
        int selectors = (acceptors /= 2);
        selectorManager = new SelectorManager(selectors);
        workerManager = new WorkerManager(workers, new DefaultRoundRobinStrategy<>());
        this.acceptors = acceptors;
        selectorManager.setWorkerManager(workerManager);
    }

    /**
     * Configures whether the server listens in blocking mode.
     */
    public Server configureBlocking(boolean block) {
        this.block = block;
        return this;
    }

    /**
     * Binds this server to the given port.
     */
    public Server bind(int port) {
        if (port < 1)
            throw new IllegalArgumentException(
                    "The port must be greater than 0.");
        try {
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.socket().setReuseAddress(true);
            serverSocketChannel.bind(new InetSocketAddress(port));
            serverSocketChannel.configureBlocking(block);
        } catch (IOException e) {
            logger.error("Failed to bind to port {}.", port);
            System.exit(1);
        }
        return this;
    }

    public Server setHandlers(Handler... handlers) {
        if (handlers.length < 1) {
            throw new IllegalArgumentException("No handlers specified.");
        }
        this.handlers.addAll(Arrays.asList(handlers));
        return this;
    }

    /**
     * Starts the server.
     */
    @Override
    public void start() {
        if (handlers.isEmpty()) {
            throw new IllegalArgumentException("No handlers specified.");
        }
        workerManager.start();
        selectorManager.start();
        for (int i = 0; i < acceptors; i++)
            executor.execute(new Acceptor());
        logger.info("Server started successfully.");
    }

    @Override
    public void close() {
        workerManager.close();
        selectorManager.close();

        executor.shutdown();

        closed = true;
        CloseableUtils.closeQuietly(serverSocketChannel);
    }

    /**
     * Accepts incoming client connections and registers them with a selector.
     *
     * @author skywalker
     */
    private class Acceptor implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    SocketChannel channel = serverSocketChannel.accept();
                    channel.configureBlocking(false);
                    selectorManager.chooseOne(null).register(channel, handlers);
                } catch (IOException e) {
                    if (closed) {
                        logger.debug("Close method was called, acceptor exiting.");
                        break;
                    } else {
                        logger.debug("Failed to accept a client connection: {}", e.getMessage());
                    }
                }
            }
        }

    }

}
