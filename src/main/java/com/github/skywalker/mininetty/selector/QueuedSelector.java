package com.github.skywalker.mininetty.selector;

import com.github.skywalker.mininetty.handler.Handler;
import com.github.skywalker.mininetty.handler.HandlerChain;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.skywalker.mininetty.manager.ChooseStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.skywalker.mininetty.util.CloseableUtils;
import com.github.skywalker.mininetty.worker.Worker;
import com.github.skywalker.mininetty.worker.WorkerManager;
import com.github.skywalker.mininetty.context.ChannelHandlerContext;
import com.github.skywalker.mininetty.lifecycle.LifeCycle;

/**
 * A selector that owns a dedicated job queue so tasks can be submitted to run on its thread.
 *
 * @author skywalker
 */
public final class QueuedSelector implements Runnable, LifeCycle {

    private Selector selector;
    private WorkerManager workerManager;

    private volatile boolean closed = false;

    private final BlockingQueue<Runnable> jobs;
    private final static int defaultQueueSize = 1024;

    private final Runnable eventProcessor = new EventProcessor();

    private static final AtomicInteger threadCounter = new AtomicInteger(0);
    // Default ByteBuffer allocation size
    private static final int defaultAllocateSize = 1024;
    private static final Logger logger = LoggerFactory.getLogger(QueuedSelector.class);

    public QueuedSelector() {
        this(defaultQueueSize);
    }

    public QueuedSelector(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be greater than 0");
        }
        jobs = new ArrayBlockingQueue<>(capacity);
    }

    public void setWorkerManager(WorkerManager workerManager) {
        this.workerManager = workerManager;
    }

    /**
     * Starts this selector in its own thread.
     */
    @Override
    public void start() {
        try {
            selector = Selector.open();
            new Thread(this, getThreadName()).start();
        } catch (IOException e) {
            logger.error("Selector open failed: {}", e.getMessage());
        }
    }

    @Override
    public void close() {
        closed = true;
        selector.wakeup();
    }

    /**
     * Register the channel to the selector with the interested ops.
     */
    public void register(SocketChannel channel, List<Handler> handlers) {
        submitTask(new RegisterChannelTask(channel, this, handlers));
    }

    public void addInterestOps(SocketChannel channel, String clientIdentifier, int ops) {
        submitTask(() -> {
            SelectionKey key = channel.keyFor(selector);
            key.interestOps(key.interestOps() | ops);
            logger.debug("Added interest ops {} for client {}.", ops, clientIdentifier);
        });
    }

    public void removeInterestOps(SocketChannel channel, String clientIdentifier, int ops) {
        submitTask(() -> {
            SelectionKey key = channel.keyFor(selector);
            key.interestOps(key.interestOps() & ~ops);
            logger.debug("Removed interest ops {} for client {}.", ops, clientIdentifier);
        });
    }

    public void unregister(SocketChannel channel, String clientIdentifier) {
        submitTask(() -> {
            SelectionKey key = channel.keyFor(selector);
            key.cancel();
            CloseableUtils.closeQuietly(channel);
            logger.debug("Unregistered channel for client {}.", clientIdentifier);
        });
    }

    @Override
    public void run() {
        while (!closed) {
            Runnable task;
            task = jobs.poll();
            if (task == null)
                eventProcessor.run();
            else
                task.run();
        }

        CloseableUtils.closeQuietly(selector);
    }

    private void submitTask(Runnable task) {
        try {
            jobs.put(task);
        } catch (InterruptedException e) {
            close();
            Thread.currentThread().interrupt();
        }
        // Wake up the selector so the task is processed without waiting for the next select().
        selector.wakeup();
    }

    private String getThreadName() {
        return String.format("queued-selector-%d", threadCounter.getAndIncrement());
    }

    /**
     * Registers a newly accepted channel with the selector and bootstraps its per-connection state:
     * builds the handler chain, binds the channel to a worker and creates the
     * {@link ChannelHandlerContext} attached to the selection key.
     */
    private class RegisterChannelTask implements Runnable {

        private final SocketChannel channel;
        private final QueuedSelector queuedSelector;
        private final List<Handler> handlers;

        public RegisterChannelTask(SocketChannel channel, QueuedSelector queuedSelector, List<Handler> handlers) {
            this.channel = channel;
            this.queuedSelector = queuedSelector;
            this.handlers = handlers;
        }

        @Override
        public void run() {
            SelectionKey key = null;
            try {
                key = channel.register(selector, SelectionKey.OP_READ);
            } catch (IOException e) {
                logger.debug("Failed to register key {}.", e.getMessage());
                try {
                    channel.close();
                } catch (IOException ignored) {
                }
            }

            if (key == null) {
                return;
            }

            HandlerChain handlerChain = new HandlerChain();
            handlerChain.addHandlers(this.handlers);
            Worker worker = workerManager.chooseOne(channel, ChooseStrategy.PostAction.BIND);

            ChannelHandlerContext context;
            try {
                context = new ChannelHandlerContext(handlerChain, worker, channel, queuedSelector);
            } catch (IOException e) {
                logger.debug("Failed to create channel handler context {}.", e.getMessage());
                key.cancel();
                try {
                    channel.close();
                } catch (IOException ignored) {
                }
                return;
            }
            key.attach(context);

            context.fireChannelActive();
        }

    }

    /**
     * Processes the events reported by the underlying selector.
     */
    private class EventProcessor implements Runnable {

        @Override
        public void run() {
            try {
                int i = selector.select();
                if (i > 0) {
                    Set<SelectionKey> keys = selector.selectedKeys();
                    for (SelectionKey key : keys) {
                        if (key.isReadable()) {
                            SocketChannel channel = (SocketChannel) key
                                    .channel();
                            ByteBuffer buffer = ByteBuffer
                                    .allocate(defaultAllocateSize);
                            int n = channel.read(buffer);
                            if (n == -1) {
                                processInActive(key);
                            } else {
                                processRead(buffer, key);
                            }
                        } else if (key.isWritable()) {
                            processWrite(key);
                        }
                    }
                    keys.clear();
                }
            } catch (IOException e) {
                logger.debug("Selector was closed.");
                closed = true;
            }
        }

        /**
         * Handles a closed client connection.
         */
        private void processInActive(SelectionKey key) {
            ChannelHandlerContext context = (ChannelHandlerContext) key.attachment();
            context.fireChannelInActive();
        }

        /**
         * Handles a read event.
         *
         * @param buffer the bytes read from the channel
         */
        private void processRead(ByteBuffer buffer, SelectionKey key) {
            ChannelHandlerContext context = (ChannelHandlerContext) key.attachment();
            context.fireChannelRead(buffer);
        }

        private void processWrite(SelectionKey key) {
            ChannelHandlerContext context = (ChannelHandlerContext) key.attachment();
            context.fireChannelWrite();
        }
    }

}
