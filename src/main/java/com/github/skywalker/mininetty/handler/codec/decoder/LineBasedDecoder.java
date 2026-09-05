package com.github.skywalker.mininetty.handler.codec.decoder;

import com.github.skywalker.mininetty.handler.HandlerInitializer;

/**
 * A decoder based on the newline character, should be used together with
 * {@link HandlerInitializer}.
 * The decoded data is passed to the next handler as {@code byte[]}; the newline
 * character is not included.
 * 
 * @author skywalker
 *
 */
public class LineBasedDecoder extends DelimiterFrameDecoder {

    public LineBasedDecoder() {
        this(DEFAULT_MAX_FRAME_LENGTH);
    }

    public LineBasedDecoder(int maxFrameLength) {
        super(maxFrameLength);
    }

    @Override
    protected byte terminator() {
        return '\n';
    }

    @Override
    protected boolean stripCarriageReturn() {
        return true;
    }

}
