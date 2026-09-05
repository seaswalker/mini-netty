package com.github.skywalker.mininetty.selector;

import com.github.skywalker.mininetty.context.FutureAttached;
import com.github.skywalker.mininetty.handler.Handler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.skywalker.mininetty.handler.HandlerChain;
import com.github.skywalker.mininetty.worker.Worker;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.skywalker.mininetty.util.CloseableUtils;
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
    public Future<?> registerServerChannelAsync(int port, List<Handler> handlers) {
        CompletableFuture<?> future = new CompletableFuture<>();
        try {
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.socket().bind(new InetSocketAddress(port));
            submitTask(new RegisterServerChannelTask(serverSocketChannel, handlers, future));
        } catch (IOException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    public Future<?> registerClientChannelAsync(String host, int port, List<Handler> handlers) {
        CompletableFuture<?> future = new CompletableFuture<>();
        try {
            SocketAddress socketAddress = new InetSocketAddress(host, port);
            SocketChannel socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(false);
            submitTask(new RegisterClientChannelTask(socketChannel, socketAddress, handlers, future));
        } catch (IOException e) {
            future.completeExceptionally(e);
        }
        return future;
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

    private interface ChannelRegistrationListener {
        void onRegistered(SelectionKey selectionKey) throws IOException;
        void onExceptionCaught(Throwable cause);
    }

    /**
     * Registers a channel with the selector.
     */
    private class RegisterChannelTask implements Runnable {

        private final SelectableChannel channel;
        private final int interestOps;
        private final ChannelRegistrationListener channelRegistrationListener;

        RegisterChannelTask(SelectableChannel channel, int interestOps, ChannelRegistrationListener channelRegistrationListener) {
            this.channel = channel;
            this.interestOps = interestOps;
            this.channelRegistrationListener = channelRegistrationListener;
        }

        @Override
        public void run() {
            try {
                SelectionKey key = channel.register(selector, interestOps);
                channelRegistrationListener.onRegistered(key);
            } catch (Exception e) {
                CloseableUtils.closeQuietly(channel);
                channelRegistrationListener.onExceptionCaught(e);
            }
        }

    }

    private static class PreClientConnectedChannelContext implements FutureAttached {
        final List<Handler> handlers;
        final CompletableFuture<?> future;
        PreClientConnectedChannelContext(List<Handler> handlers, CompletableFuture<?> future) {
            this.handlers = handlers;
            this.future = future;
        }
        @Override
        public CompletableFuture<?> getFuture() {
            return future;
        }
    }

    private PreClientConnectedChannelContext createPreClientConnectedChannelContext(List<Handler> handlers, CompletableFuture<?> completableFuture) {
        return new PreClientConnectedChannelContext(handlers, completableFuture);
    }

    private class RegisterServerChannelTask extends RegisterChannelTask {
        RegisterServerChannelTask(ServerSocketChannel serverSocketChannel, List<Handler> handlers, CompletableFuture<?> completableFuture) {
            super(serverSocketChannel, SelectionKey.OP_ACCEPT, new ChannelRegistrationListener() {
                @Override
                public void onRegistered(SelectionKey selectionKey) {
                    PreClientConnectedChannelContext preClientConnectedChannelContext = createPreClientConnectedChannelContext(handlers, completableFuture);
                    selectionKey.attach(preClientConnectedChannelContext);
                    completableFuture.whenComplete((unused, throwable) -> {
                        if (completableFuture.isCancelled()) {
                            submitTask(() -> cancelKey(selectionKey, null));
                        }
                    });
                }
                @Override
                public void onExceptionCaught(Throwable cause) {
                    completableFuture.completeExceptionally(cause);
                }
            });
        }
    }

    private class RegisterClientChannelTask extends RegisterChannelTask {
        RegisterClientChannelTask(SocketChannel socketChannel, SocketAddress socketAddress, List<Handler> handlers,
                                  CompletableFuture<?> channelRegisterFuture) {
            super(socketChannel, SelectionKey.OP_CONNECT, new ChannelRegistrationListener() {
                @Override
                public void onRegistered(SelectionKey selectionKey) throws IOException {
                    PreClientConnectedChannelContext context = new PreClientConnectedChannelContext(handlers, channelRegisterFuture);
                    selectionKey.attach(context);
                    socketChannel.connect(socketAddress);
                    channelRegisterFuture.whenComplete((unused, throwable) -> {
                        if (channelRegisterFuture.isCancelled()) {
                            submitTask(() -> cancelKey(selectionKey, null));
                        }
                    });
                }
                @Override
                public void onExceptionCaught(Throwable cause) {
                    channelRegisterFuture.completeExceptionally(cause);
                }
            });
        }
    }

    private ChannelHandlerContext createSocketChannelContext(SocketChannel socketChannel, List<Handler> handlers,
                                                             CompletableFuture<?> completableFuture) throws IOException {
        HandlerChain handlerChain = new HandlerChain();
        handlerChain.addHandlers(handlers);
        Worker worker = workerManager.chooseOne();
        return new ChannelHandlerContext(
                handlerChain, worker, socketChannel, this, completableFuture
        );
    }

    /**
     * Processes the events reported by the underlying selector.
     */
    private class EventProcessor implements Runnable {

        @Override
        public void run() {
            int i = 0;
            try {
                i = selector.select();
            } catch (IOException e) {
                logger.warn("Failed to select events from selector.", e);
            }

            if (i > 0) {
                Set<SelectionKey> keys = selector.selectedKeys();
                for (SelectionKey key : keys) {
                    if (key.isConnectable()) {
                        processConnectable(key);
                    } else if (key.isReadable()) {
                        processReadable(key);
                    } else if (key.isWritable()) {
                        processWritable(key);
                    } else if (key.isAcceptable()) {
                        processAcceptable(key);
                    }
                }
                keys.clear();
            }
        }

        private void processReadable(SelectionKey key) {
            if (extractFuture(key).isCancelled()) {
                extractSocketChannelAttachment(key).fireChannelInActive();
                return;
            }

            SocketChannel channel = (SocketChannel) key.channel();
            ByteBuffer buffer = ByteBuffer.allocate(defaultAllocateSize);
            try {
                int n = channel.read(buffer);
                if (n == -1) {
                    processInActive(key);
                } else {
                    fireChannelRead(buffer, key);
                }
            } catch (IOException e) {
                cancelKey(key, e);
            }
        }

        private void processConnectable(SelectionKey key) {
            if (extractFuture(key).isCancelled()) {
                cancelKey(key, null);
                return;
            }

            SocketChannel channel = (SocketChannel) key.channel();
            PreClientConnectedChannelContext preClientConnectedChannelContext = (PreClientConnectedChannelContext) key.attachment();

            try {
                if (channel.finishConnect()) {
                    key.interestOps(SelectionKey.OP_READ);
                    ChannelHandlerContext context = createSocketChannelContext(
                            channel,
                            preClientConnectedChannelContext.handlers,
                            preClientConnectedChannelContext.future
                    );
                    key.attach(context);
                    processActive(key);
                }
            } catch (IOException e) {
                cancelKey(key, e);
            }
        }

        private void processAcceptable(SelectionKey key) {
            if (extractFuture(key).isCancelled()) {
                cancelKey(key, null);
                return;
            }

            ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
            PreClientConnectedChannelContext preClientConnectedChannelContext = (PreClientConnectedChannelContext) key.attachment();

            SocketChannel socketChannel;
            try {
               socketChannel = serverSocketChannel.accept();
            } catch (IOException e) {
                logger.error("Server failed to accept connection.", e);
                cancelKey(key, e);
                return;
            }

            if (socketChannel == null) {
                return;
            }

            String clientIdentifier = null;
            ChannelHandlerContext context;

            try {
                socketChannel.configureBlocking(false);
                clientIdentifier = socketChannel.getRemoteAddress().toString();
                SelectionKey acceptedKey = socketChannel.register(selector, SelectionKey.OP_READ);
                context = createSocketChannelContext(socketChannel, preClientConnectedChannelContext.handlers, preClientConnectedChannelContext.future);
                acceptedKey.attach(context);
            } catch (IOException e) {
                logger.warn("Failed to register accepted channel: {}.", clientIdentifier,  e);
                return;
            }

            context.fireChannelActive();
        }

        private void processActive(SelectionKey key) {
            if (extractFuture(key).isCancelled()) {
                cancelKey(key, null);
                return;
            }
            ChannelHandlerContext context = (ChannelHandlerContext) key.attachment();
            context.fireChannelActive();
        }

        /**
         * Handles a closed client connection.
         */
        private void processInActive(SelectionKey key) {
            ChannelHandlerContext context = (ChannelHandlerContext) key.attachment();
            context.fireChannelInActive();
        }

        private void processWritable(SelectionKey key) {
            if (extractFuture(key).isCancelled()) {
                extractSocketChannelAttachment(key).fireChannelInActive();
                return;
            }
            extractSocketChannelAttachment(key).fireChannelWrite();
        }

        /**
         * Handles a read event.
         *
         * @param buffer the bytes read from the channel
         */
        private void fireChannelRead(ByteBuffer buffer, SelectionKey key) {
            extractSocketChannelAttachment(key).fireChannelRead(buffer);
        }

    }

    private static ChannelHandlerContext extractSocketChannelAttachment(SelectionKey key) {
        return (ChannelHandlerContext) key.attachment();
    }

    private static CompletableFuture<?> extractFuture(SelectionKey key) {
        return ((FutureAttached) key.attachment()).getFuture();
    }

    private static void cancelKey(SelectionKey key, @Nullable Throwable cause) {
        if (cause != null) {
            extractFuture(key).completeExceptionally(cause);
        } else {
            extractFuture(key).complete(null);
        }
        key.cancel();
        CloseableUtils.closeQuietly(key.channel());
    }

}
