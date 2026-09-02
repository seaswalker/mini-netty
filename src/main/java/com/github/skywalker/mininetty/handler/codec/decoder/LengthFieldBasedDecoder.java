package com.github.skywalker.mininetty.handler.codec.decoder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import com.github.skywalker.mininetty.context.MessageProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.skywalker.mininetty.handler.HandlerInitializer;
import com.github.skywalker.mininetty.handler.InBoundHandlerAdapter;

/**
 * Reads the amount of data declared by a length field, solving packet sticking and
 * half-packet problems.
 * <p>The decoded data is passed to the next handler as a {@code byte[]}. This handler
 * should be used together with {@link HandlerInitializer}.</p>
 * 
 * @author skywalker
 *
 */
public class LengthFieldBasedDecoder extends InBoundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(LengthFieldBasedDecoder.class);

    // Byte order of the length field
    private final ByteOrder byteOrder;
    // Start offset of the length field within the frame
    private final int offset;
    // Length (in bytes) of the length field
    private final int length;
    // Maximum allowed content length
    private final int maxLength;
    private final int dataOffset;
    private State state = State.INIT;
    private byte[] todo;
    // How many more bytes are still needed
    private int needed = 0;
    private int neededOffset = 0;
    private static final int defaultMaxLength = 2048;

    public LengthFieldBasedDecoder(int offset, int length) {
        this(offset, length, defaultMaxLength);
    }

    public LengthFieldBasedDecoder(ByteOrder byteOrder, int offset, int length) {
        this(byteOrder, offset, length, defaultMaxLength);
    }

    public LengthFieldBasedDecoder(int offset, int length, int maxLength) {
        this(ByteOrder.LITTLE_ENDIAN, offset, length, maxLength);
    }

    public LengthFieldBasedDecoder(ByteOrder byteOrder, int offset, int length,
            int maxLength) {
        if (byteOrder == null) byteOrder = ByteOrder.LITTLE_ENDIAN;
        this.byteOrder = byteOrder;
        this.offset = offset;
        this.length = length;
        this.dataOffset = offset + length;
        this.maxLength = maxLength - this.dataOffset;
    }

    @Override
    public void channelRead(Object message, MessageProcessingContext context) {
        if (message instanceof ByteBuffer) {
            ByteBuffer buffer = (ByteBuffer) message;
            if (buffer.hasArray()) {
                buffer.flip();
                List<byte[]> out = new ArrayList<>();
                int remaining = buffer.remaining();
                switch (state) {
                case INIT:
                    process(buffer, 0, out);
                    break;
                case HEAD_NEEDED:
                    // Complete the buffered head with the new bytes
                    if (remaining < needed) {
                        // Still not enough for a full head
                        System.arraycopy(buffer.array(), 0, todo, neededOffset, remaining);
                        neededOffset += remaining;
                    } else if (remaining == needed) {
                        System.arraycopy(buffer.array(), 0, todo, neededOffset, remaining);
                        state = State.CONTENT_NEEDED;
                        int contentLength = parseAndCheckContentLength();
                        byte[] arr = new byte[dataOffset + contentLength];
                        System.arraycopy(todo, 0, arr, 0, dataOffset);
                        todo = arr;
                        needed = contentLength;
                        neededOffset = dataOffset;
                    } else {
                        System.arraycopy(buffer.array(), 0, todo, neededOffset, remaining);
                        int contentLength = parseAndCheckContentLength();
                        byte[] arr = new byte[dataOffset + contentLength];
                        System.arraycopy(todo, 0, arr, 0, dataOffset);
                        remaining -= dataOffset;
                        if (remaining < contentLength) {
                            System.arraycopy(buffer.array(), needed, arr, dataOffset, remaining);
                            state = State.CONTENT_NEEDED;
                            todo = arr;
                            needed = contentLength - remaining;
                            neededOffset = remaining;
                        } else if (remaining == contentLength) {
                            System.arraycopy(buffer.array(), needed, arr, dataOffset, remaining);
                            out.add(arr);
                            todo = null;
                            state = State.INIT;
                        } else {
                            System.arraycopy(buffer.array(), needed, arr, dataOffset, contentLength);
                            out.add(arr);
                            todo = null;
                            process(buffer, needed + contentLength, out);
                        }
                    }
                    break;
                case CONTENT_NEEDED:
                    if (remaining < needed) {
                        // Still not enough content
                        System.arraycopy(buffer.array(), 0, todo, neededOffset, remaining);
                        neededOffset += remaining;
                        needed -= remaining;
                    } else if (remaining == needed) {
                        System.arraycopy(buffer.array(), 0, todo, neededOffset, remaining);
                        state = State.INIT;
                        out.add(todo);
                        todo = null;
                    } else {
                        System.arraycopy(buffer.array(), 0, todo, neededOffset, needed);
                        out.add(todo);
                        process(buffer, needed, out);
                    }
                }
                context.forkedChannelRead(out);
            } else {
                logger.debug("We support heap ByteBuffer only.");
            }
        } else {
            context.channelRead(message);
        }
    }

    private int parseAndCheckContentLength() {
        int contentLength = bytes2Int(todo, offset, length);
        if (contentLength > maxLength) {
            throw new IllegalStateException(String.format("The content length: %d exceeds the max length: %d", contentLength, maxLength));
        }
        return contentLength;
    }

    /**
     * Processes a batch of data starting from a head.
     *
     * @param buffer the byte buffer
     * @param begin  the index to start processing from
     * @param out    the result collection
     */
    private void process(ByteBuffer buffer, int begin, List<byte[]> out) {
        // Total amount of readable data
        int limit = buffer.limit(), remaining = limit - begin;
        byte[] array = buffer.array();
        while (begin < limit) {
            // The head is incomplete
            if (remaining < dataOffset) {
                byte[] head = new byte[dataOffset];
                System.arraycopy(array, begin, head, 0, remaining);
                state = State.HEAD_NEEDED;
                needed = dataOffset - remaining;
                neededOffset = remaining;
                todo = head;
                break;
            }
            int contentLength = bytes2Int(array, begin + offset, length);
            if (contentLength > maxLength) {
                throw new IllegalStateException(String.format("The content length: %d exceeds the max length: %d", contentLength, maxLength));
            }
            remaining -= dataOffset;
            byte[] result = new byte[dataOffset + contentLength];
            if (remaining < contentLength) {
                System.arraycopy(array, begin, result, 0, dataOffset + remaining);
                state = State.CONTENT_NEEDED;
                todo = result;
                needed = contentLength - remaining;
                neededOffset = dataOffset + remaining;
                break;
            }
            if (remaining == contentLength) {
                System.arraycopy(array, begin, result, 0, dataOffset + remaining);
                state = State.INIT;
                needed = 0;
                todo = null;
                out.add(result);
                break;
            }
            int total = dataOffset + contentLength;
            System.arraycopy(array, begin, result, 0, total);
            out.add(result);
            remaining -= contentLength;
            begin += total;
        }
    }

    /**
     * Converts a byte array to an int value.
     *
     * @param data the byte array
     * @return the int value
     */
    private int bytes2Int(byte[] data, int offset, int length) {
        int result = 0;
        if (byteOrder == ByteOrder.LITTLE_ENDIAN) {
            for (int i = 0; i < length; i++) {
                result |= ((data[offset + i] & 0xff) << (i << 3));
            }
        } else {
            for (int i = 0; i < length; i++) {
                result |= ((data[offset + i] & 0xff) << ((length - 1 - i) << 3));
            }
        }
        return result;
    }

    private enum State {
        // Initial state; the head needs to be read first
        INIT,
        CONTENT_NEEDED,
        HEAD_NEEDED
    }

}
