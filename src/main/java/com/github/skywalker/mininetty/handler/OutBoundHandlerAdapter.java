package com.github.skywalker.mininetty.handler;

import com.github.skywalker.mininetty.context.MessageProcessingContext;

/**
 * Adapter that does nothing but forward events down the chain.
 * Custom {@link OutBoundHandler}s may extend this class.
 *
 * @author skywalker
 *
 */
public class OutBoundHandlerAdapter implements OutBoundHandler {

    @Override
    public void channelWrite(Object message, MessageProcessingContext context) {
        context.channelWrite(message);
    }

    @Override
    public void channelInActive(MessageProcessingContext context) {
        context.channelInactive();
    }

    @Override
    public void channelActive(MessageProcessingContext context) {
        context.channelActive();
    }

    @Override
    public void channelExceptionCaught(MessageProcessingContext context, Exception e) {
        context.channelExceptionCaught(e);
    }
}
