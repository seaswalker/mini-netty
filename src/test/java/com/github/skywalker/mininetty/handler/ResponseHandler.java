package com.github.skywalker.mininetty.handler;

import com.github.skywalker.mininetty.context.MessageProcessingContext;

/**
 * Echoes received messages back to the client for testing.
 * 
 * @author skywalker
 *
 */
public class ResponseHandler extends InBoundHandlerAdapter {

    @Override
    public void channelRead(Object message, MessageProcessingContext context) {
        // Logging every echo would slow down the performance test significantly,
        // so no logging is performed here
        context.channel().writeAndFlush((String) message + "\n");
    }

}
