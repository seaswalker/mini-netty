package com.github.skywalker.mininetty.handler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.Deque;
import java.util.LinkedList;

import com.github.skywalker.mininetty.context.ChannelHandlerContext;
import com.github.skywalker.mininetty.context.MessageProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for actually sending the data to the client, as the last
 * outbound handler in the chain.
 *
 * @author skywalker
 */
public class DefaultOutBoundHandler extends OutBoundHandlerAdapter {

    private final Deque<ByteBuffer> pending = new LinkedList<>();

    private boolean writeOpsRegistered = false;
    private int pendingSize;

    private static final Logger logger = LoggerFactory.getLogger(DefaultOutBoundHandler.class);
    private static final int defaultMaxPendingSize = 1024 * 20;

    @Override
    public void channelWrite(Object message, MessageProcessingContext context) {
        if (message == null) return;
        ByteBuffer result = null;

        if (message instanceof ByteBuffer) {
            result = (ByteBuffer) message;
        } else if (message instanceof byte[]) {
            result = ByteBuffer.wrap((byte[]) message);
        } else if (message instanceof String) {
            result = ByteBuffer.wrap(message.toString().getBytes());
        }
        if (result == null) {
            throw new IllegalStateException("Unsupported type: " + message.getClass().getName());
        }

        pending.offer(result);
        pendingSize += result.remaining();

        drainPending(context.channel());
    }

    @Override
    public void channelInActive(MessageProcessingContext context) {
        context.channel().close();
    }

    public void drainPending(ChannelHandlerContext context) {
        ByteBuffer buffer;
        int writtenTotal = 0;

        try {
            while ((buffer = pending.poll()) != null) {
                int remaining = buffer.remaining();
                int written = writeBuffer(buffer, context);
                writtenTotal += written;
                if (written < remaining) {
                    pending.offerFirst(buffer);
                    // Wait until the write buffer becomes available
                    context.addInterestOps(SelectionKey.OP_WRITE);
                    writeOpsRegistered = true;
                    break;
                }
            }

            pendingSize -= writtenTotal;
            if (pendingSize > defaultMaxPendingSize) {
                logger.warn(
                        "Client: {} write pending size: {} exceeded defaultMaxPendingSize: {}, disable writing.",
                        context.getClientIdentifier(), pendingSize, defaultMaxPendingSize
                );
                context.setWritable(false);
            } else {
                context.setWritable(true);
            }

            // All data written, so remove the write interest ops
            if (writeOpsRegistered) {
                context.removeInterestOps(SelectionKey.OP_WRITE);
                writeOpsRegistered = false;
            }
        } catch (IOException e) {
            logger.debug("Failed to drain pending buffer for: {}, channel closed?", context.getClientIdentifier(), e);
            context.fireChannelInActive();
        }
    }

    private int writeBuffer(ByteBuffer buffer, ChannelHandlerContext context) throws IOException {
        int remaining = buffer.remaining();
        while (buffer.hasRemaining()) {
            int written = context.getSocketChannel().write(buffer);
            if (written == 0) {
                logger.debug(
                        "Send buffer full, remote address: {}, pending: {} bytes.",
                        context.getClientIdentifier(), buffer.remaining()
                );
                break;
            }
        }
        return remaining - buffer.remaining();
    }

}
