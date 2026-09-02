package com.github.skywalker.mininetty.handler.codec.encoder;

import java.nio.ByteBuffer;

import com.github.skywalker.mininetty.context.MessageProcessingContext;
import com.github.skywalker.mininetty.handler.OutBoundHandlerAdapter;

/**
 * Converts a String to a {@link ByteBuffer}.
 * 
 * @author skywalker
 *
 */
public class StringEncoder extends OutBoundHandlerAdapter {

    @Override
    public void channelWrite(Object message, MessageProcessingContext context) {
        if (message instanceof String) {
            context.channelWrite(ByteBuffer.wrap(((String) message).getBytes()));
        } else {
            context.channelWrite(message);
        }
    }

}
