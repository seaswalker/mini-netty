package com.github.skywalker.mininetty.handler.codec.decoder;

import com.github.skywalker.mininetty.context.MessageProcessingContext;
import com.github.skywalker.mininetty.handler.HandlerInitializer;
import com.github.skywalker.mininetty.handler.InBoundHandlerAdapter;
import org.jspecify.annotations.NonNull;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 * A decoder based on the newline character, should be used together with
 * {@link HandlerInitializer}.
 * The decoded data is passed to the next handler as {@code byte[]}; the newline
 * character is not included.
 * 
 * @author skywalker
 *
 */
public class LineBasedDecoder extends InBoundHandlerAdapter {

    private final byte[] pendingBuffer;

    private int index = 0;
    private boolean prevR = false;

    private static final int defaultMaxFrameLength = 2048;
    private static final byte[] empty = new byte[0];

    public LineBasedDecoder() {
        this(defaultMaxFrameLength);
    }

    public LineBasedDecoder(int maxFrameLength) {
        if (maxFrameLength <= 0) {
            throw new IllegalArgumentException("maxFrameLength must be greater than 0");
        }
        this.pendingBuffer = new byte[maxFrameLength];
    }


    @Override
    public void channelRead(Object message, MessageProcessingContext context) {
        if (!(message instanceof ByteBuffer)) {
            super.channelRead(message, context);
            return;
        }

        ByteBuffer buffer = (ByteBuffer) message;
        buffer.flip();

        List<byte[]> out = new LinkedList<>();
        int frameBeginIndex = 0;
        byte[] array = buffer.array();
        int limit = buffer.limit();
        boolean frameEnded = false;

        for (int i = 0; i < limit; i++) {
            if (frameEnded) {
                frameEnded = false;
                frameBeginIndex = i;
            }
            byte b = array[i];
            if (b == '\n') {
                byte[] frame = frameEnded(frameBeginIndex, i, array);
                out.add(frame);
                frameEnded = true;
                prevR = false;
            } else {
                prevR = (b == '\r');
            }
        }

        if (!frameEnded) {
            handleRemainingBytes(frameBeginIndex, limit, array);
        }

        if (!out.isEmpty()) {
            context.forkedChannelRead(out);
        }
    }

    private byte @NonNull [] frameEnded(int frameBeginIndex, int frameEndIndex, byte[] array) {
        if (prevR) {
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

    private void handleRemainingBytes(int frameBeginIndex, int limit, byte[] array) {
        int length = limit - frameBeginIndex;
        if (length + index > pendingBuffer.length) {
            throw new IllegalArgumentException("Frame length is greater than maxFrameLength: " + pendingBuffer.length);
        }
        System.arraycopy(array, frameBeginIndex, pendingBuffer, index, length);
        index += length;
    }

}