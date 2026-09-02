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
public class LineBasedDecoder extends DelimiterBasedDecoder {

    public LineBasedDecoder() {
        //todo: \r\n is not a single character and {@link DelimiterBasedDecoder}
        //only supports ASCII delimiters, so use '\n' alone for now
        this('\n');
    }

    private LineBasedDecoder(char delimiter) {
        super(delimiter);
    }

}