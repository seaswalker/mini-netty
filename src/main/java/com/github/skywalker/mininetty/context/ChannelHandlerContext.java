package com.github.skywalker.mininetty.context;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.github.skywalker.mininetty.handler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.skywalker.mininetty.selector.QueuedSelector;
import com.github.skywalker.mininetty.worker.Worker;

/**
 * Per-connection execution context that forwards events through the handler chain.
 *
 * @author skywalker
 */
public class ChannelHandlerContext implements FutureAttached {

    private final HandlerChain handlerChain;
    private final Worker worker;
    private final SocketChannel channel;
    private final QueuedSelector queuedSelector;
    private final String clientIdentifier;
    private final CompletableFuture<?> future;

    private boolean running = true;
    private boolean writable = true;
    private long lastActiveTime = System.nanoTime();

    private static final long defaultIdleTimeoutSeconds = 5;
    private static final Logger logger = LoggerFactory.getLogger(ChannelHandlerContext.class);

    public ChannelHandlerContext(HandlerChain handlerChain, Worker worker, SocketChannel channel,
                                 QueuedSelector queuedSelector, CompletableFuture<?> future) throws IOException {
        this.handlerChain = handlerChain;
        this.worker = worker;
        this.channel = channel;
        this.queuedSelector = queuedSelector;
        this.clientIdentifier = channel.getRemoteAddress().toString();
        this.future = future;
    }

    public boolean isRunning() {
        return running;
    }

    public void stop() {
        this.running = false;
    }

    public boolean isWritable() {
        return writable;
    }

    public void setWritable(boolean writable) {
        this.writable = writable;
    }

    /**
     * Fires the channel active event, which also schedules the idle detection task.
     */
    public void fireChannelActive() {
        worker.schedule(
                new Runnable() {
                    @Override
                    public void run() {
                        long idleTime = System.nanoTime() - getLastActiveTime();
                        if (idleTime > TimeUnit.SECONDS.toNanos(defaultIdleTimeoutSeconds)) {
                            logger.info("Client: {} idle for more than {}s, closing the channel.", clientIdentifier, defaultIdleTimeoutSeconds);
                            fireChannelInActive();
                            return;
                        }
                        worker.schedule(this, defaultIdleTimeoutSeconds, TimeUnit.SECONDS);
                    }
                },
                defaultIdleTimeoutSeconds,
                TimeUnit.SECONDS
        );
        runInWorker(() -> {
            MessageProcessingContext messageProcessingContext = new MessageProcessingContext(this);
            messageProcessingContext.channelActive();
        });
    }

    /**
     * Fires the channel inactive event.
     */
    public void fireChannelInActive() {
        runInWorker(() -> {
            MessageProcessingContext messageProcessingContext = new MessageProcessingContext(this);
            messageProcessingContext.channelInactive();
            stop();
        });
    }

    /**
     * Fires the channel read event with the given message.
     *
     * @param message the message read from the channel
     */
    public void fireChannelRead(Object message) {
        runInWorker(() -> {
            MessageProcessingContext messageProcessingContext = new MessageProcessingContext(this);
            messageProcessingContext.channelRead(message);
        });
    }

    /**
     * Fires the channel write event, triggering the outbound handler chain.
     */
    public void fireChannelWrite() {
        runInWorker(() -> handlerChain.getDefaultOutBoundHandler().drainPending(this));
    }

    /**
     * Writes data back to the client, triggering the outbound events.
     *
     * @param message the data to write
     */
    public void writeAndFlush(Object message) {
        runInWorker(() -> {
            if (isWritable()) {
                MessageProcessingContext messageProcessingContext = new MessageProcessingContext(this);
                messageProcessingContext.channelWrite(message);
            }
        });
    }

    public void fireExceptionCaught(Exception cause) {
        runInWorker(() -> {
            MessageProcessingContext messageProcessingContext = new MessageProcessingContext(this);
            messageProcessingContext.channelExceptionCaught(cause);
        });
    }

    public void addInterestOps(int ops) {
        queuedSelector.addInterestOps(channel, clientIdentifier, ops);
    }

    public void removeInterestOps(int ops) {
        queuedSelector.removeInterestOps(channel, clientIdentifier, ops);
    }

    public void close() {
        queuedSelector.unregister(channel, clientIdentifier);
    }

    public HandlerChain getHandlerChain() {
        return handlerChain;
    }

    public SocketChannel getSocketChannel() {
        return channel;
    }

    public String getClientIdentifier() {
        return clientIdentifier;
    }

    public void setLastActiveTime(long lastActiveTime) {
        this.lastActiveTime = lastActiveTime;
    }

    public long getLastActiveTime() {
        return lastActiveTime;
    }

    @Override
    public CompletableFuture<?> getFuture() {
        return future;
    }

    private void runInWorker(Runnable task) {
        if (worker.isInWorker()) {
            task.run();
        } else {
            worker.submit(task);
        }
    }

}
