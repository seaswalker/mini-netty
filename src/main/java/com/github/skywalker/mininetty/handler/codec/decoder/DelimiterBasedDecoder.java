package com.github.skywalker.mininetty.handler.codec.decoder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.github.skywalker.mininetty.context.MessageProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.skywalker.mininetty.handler.HandlerInitializer;
import com.github.skywalker.mininetty.handler.InBoundHandlerAdapter;

/**
 * A decoder based on a (ASCII) delimiter, should be used together with
 * {@link HandlerInitializer}.
 * The decoded data is passed to the next handler as {@code byte[]}; the delimiter
 * is not included.
 *
 * @author skywalker
 */
public class DelimiterBasedDecoder extends InBoundHandlerAdapter {

    private final byte delimiter;
    // The leftover bytes from the last batch
    private byte[] todo;
    private static final int defaultMaxLength = 1024;
    private final int maxLength;
    private static final Logger logger = LoggerFactory.getLogger(DelimiterBasedDecoder.class);

    public DelimiterBasedDecoder(char delimiter) {
        this(delimiter, 0);
    }

    public DelimiterBasedDecoder(char delimiter, int maxLength) {
        if (delimiter > 127) {
            throw new IllegalArgumentException("We support ASCII code only.");
        }
        this.delimiter = (byte) delimiter;
        if (maxLength > 0) {
            this.maxLength = maxLength;
        } else {
            this.maxLength = defaultMaxLength;
        }
    }

    @Override
    public void channelRead(Object message, MessageProcessingContext context) {
        if (message instanceof ByteBuffer) {
            ByteBuffer buffer = (ByteBuffer) message;
            if (buffer.hasArray()) {
                buffer.flip();
                byte[] array = buffer.array();
                List<byte[]> out = new ArrayList<>();
                int start = 0, i = 0;
                for (int l = buffer.limit(); i < l; i++) {
                    if (array[i] == delimiter) {
                        byte[] result;
                        if (start == 0 && todo != null) {
                            // Combine with the leftover bytes from the previous batch
                            int tl = todo.length, length = tl + i;
                            if (check(length)) return;
                            result = new byte[length];
                            System.arraycopy(todo, 0, result, 0, tl);
                            System.arraycopy(array, 0, result, tl, i);
                            todo = null;
                        } else {
                            int length = i - start;
                            if (check(length)) return;
                            result = new byte[length];
                            System.arraycopy(array, start, result, 0, length);
                        }
                        out.add(result);
                        start = i + 1;
                    }
                }
                if (array[i - 1] != delimiter) {
                    int length = i - start;
                    if (check(length)) return;
                    todo = new byte[length];
                    System.arraycopy(array, start, todo, 0, length);
                }
                context.forkedChannelRead(out);
            } else {
                logger.debug("We support heap buffer only.");
            }
        } else {
            context.channelRead(message);
        }
    }

    /**
     * Checks whether the content length has reached the maximum allowed length.
     *
     * @param i the content length
     * @return true if the maximum length has been reached
     */
    private boolean check(int i) {
        boolean result = i > maxLength;
        if (result) {
            logger.warn("The content length {} exceeds the max length {}", i, maxLength);
        }
        return result;
    }

}
