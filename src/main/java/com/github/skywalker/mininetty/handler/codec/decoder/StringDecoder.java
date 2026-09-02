package com.github.skywalker.mininetty.handler.codec.decoder;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.github.skywalker.mininetty.context.MessageProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.skywalker.mininetty.handler.InBoundHandlerAdapter;

/**
 * Converts a {@link ByteBuffer} or {@code byte[]} to a String. Only heap
 * ByteBuffers are supported.
 * 
 * @author skywalker
 *
 */
public class StringDecoder extends InBoundHandlerAdapter {

    private static final Charset defaultCharSet = StandardCharsets.UTF_8;
    private static final Logger logger = LoggerFactory.getLogger(StringDecoder.class);
    private final Charset charset;

    public StringDecoder() {
        this(null);
    }

    public StringDecoder(Charset charset) {
        if (charset != null) {
            this.charset = charset;
        } else {
            this.charset = defaultCharSet;
        }
    }

    @Override
    public void channelRead(Object message, MessageProcessingContext context) {
        if (message == null) return;
        byte[] array = null;
        if (message instanceof ByteBuffer) {
            ByteBuffer buffer = (ByteBuffer) message;
            if (buffer.hasArray()) {
                array = buffer.array();
            } else {
                logger.debug("We support heap ByteBuffer only.");
            }
        } else if (message instanceof byte[]) {
            array = (byte[]) message;
        }
        if (array != null) {
            message = new String(array, charset);
        }
        context.channelRead(message);
    }

}
