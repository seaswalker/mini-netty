package com.github.skywalker.mininetty.handler.codec.decoder;

import com.github.skywalker.mininetty.handler.HandlerInitializer;

/**
 * A decoder based on a (ASCII) delimiter, should be used together with
 * {@link HandlerInitializer}.
 * The decoded data is passed to the next handler as {@code byte[]}; the delimiter
 * is not included.
 *
 * @author skywalker
 */
public class DelimiterBasedDecoder extends DelimiterFrameDecoder {

    private final byte delimiter;

    public DelimiterBasedDecoder(byte delimiter) {
        this(delimiter, DEFAULT_MAX_FRAME_LENGTH);
    }

    public DelimiterBasedDecoder(byte delimiter, int maxFrameLength) {
        super(maxFrameLength);
        this.delimiter = delimiter;
    }

    @Override
    protected byte terminator() {
        return delimiter;
    }

}
