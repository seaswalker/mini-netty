package com.github.skywalker.mininetty.handler.codec.decoder;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import com.github.skywalker.mininetty.context.MessageProcessingContext;
import com.github.skywalker.mininetty.handler.InBoundHandlerAdapter;
import org.jspecify.annotations.NonNull;

/**
 * Base class for byte-stream decoders that split a continuous stream into frames on a
 * single terminator byte.
 *
 * <p>Each incoming buffer is scanned for the terminator; the content collected up to
 * (but not including) the terminator is emitted as a {@code byte[]} frame. A trailing
 * partial frame is buffered across reads until its terminator arrives, which solves the
 * half-packet problem. The length of a frame - both the part buffered in previous reads
 * and the part of the current read - is bounded by {@code maxFrameLength}.</p>
 *
 * <p>Subclasses supply the terminator byte via {@link #terminator()} and may opt into
 * dropping a single {@code \r} that immediately precedes the terminator (i.e. CRLF line
 * endings) via {@link #stripCarriageReturn()}.</p>
 *
 * @author skywalker
 */
abstract class DelimiterFrameDecoder extends InBoundHandlerAdapter {

    protected static final int DEFAULT_MAX_FRAME_LENGTH = 2048;

    private static final byte[] empty = new byte[0];

    // The leftover bytes from the last read, waiting for the terminator
    protected final byte[] pendingBuffer;
    // How many bytes of pendingBuffer are currently in use
    protected int index = 0;
    // Whether the byte right before the (next) terminator is '\r' (CRLF handling)
    protected boolean carriageReturnPending = false;

    protected DelimiterFrameDecoder(int maxFrameLength) {
        if (maxFrameLength <= 0) {
            throw new IllegalArgumentException("maxFrameLength must be greater than 0");
        }
        this.pendingBuffer = new byte[maxFrameLength];
    }

    /** The single byte that terminates a frame. */
    protected abstract byte terminator();

    /** Whether a {@code \r} immediately preceding the terminator should be dropped. */
    protected boolean stripCarriageReturn() {
        return false;
    }

    @Override
    public void channelRead(Object message, MessageProcessingContext context) {
        if (!(message instanceof ByteBuffer)) {
            context.channelRead(message);
            return;
        }

        ByteBuffer buffer = (ByteBuffer) message;
        buffer.flip();

        List<byte[]> out = new LinkedList<>();
        int frameBeginIndex = 0;
        byte[] array = buffer.array();
        int limit = buffer.limit();
        boolean frameEnded = false;
        byte frameTerminator = terminator();

        for (int i = 0; i < limit; i++) {
            if (frameEnded) {
                frameEnded = false;
                frameBeginIndex = i;
            }
            byte b = array[i];
            if (b == frameTerminator) {
                byte[] frame = frameEnded(frameBeginIndex, i, array);
                out.add(frame);
                frameEnded = true;
                carriageReturnPending = false;
            } else {
                carriageReturnPending = (b == '\r');
            }
        }

        if (!frameEnded) {
            handleRemainingBytes(frameBeginIndex, limit, array);
        }

        if (!out.isEmpty()) {
            context.forkedChannelRead(out);
        }
    }

    /**
     * Builds the frame ending at {@code frameEndIndex} (exclusive), merging the bytes
     * buffered across previous reads with the bytes of the current read.
     */
    private byte @NonNull [] frameEnded(int frameBeginIndex, int frameEndIndex, byte[] array) {
        if (stripCarriageReturn() && carriageReturnPending) {
            --frameEndIndex;
        }

        int currentFrameLength = frameEndIndex - frameBeginIndex;
        int frameLength = currentFrameLength + index;

        if (frameLength <= 0) {
            return empty;
        }

        if (frameLength > pendingBuffer.length) {
            throw new IllegalArgumentException("Frame length is greater than maxFrameLength: " + pendingBuffer.length);
        }

        byte[] frame = new byte[frameLength];
        if (index > 0) {
            System.arraycopy(pendingBuffer, 0, frame, 0, Math.min(index, frameLength));
        }
        if (currentFrameLength > 0) {
            System.arraycopy(array, frameBeginIndex, frame, index, currentFrameLength);
        }
        index = 0;

        return frame;
    }

    /** Buffers the unterminated tail of the current read until its terminator arrives. */
    private void handleRemainingBytes(int frameBeginIndex, int limit, byte[] array) {
        int length = limit - frameBeginIndex;
        if (length + index > pendingBuffer.length) {
            throw new IllegalArgumentException("Frame length is greater than maxFrameLength: " + pendingBuffer.length);
        }
        System.arraycopy(array, frameBeginIndex, pendingBuffer, index, length);
        index += length;
    }

}
