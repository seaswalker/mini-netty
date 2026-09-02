package com.github.skywalker.mininetty.handler;

import com.github.skywalker.mininetty.context.MessageProcessingContext;

/**
 * Adapter that does nothing but forward events down the chain.
 * Custom {@link InBoundHandler}s may extend this class.
 *
 * @author skywalker
 *
 */
public class InBoundHandlerAdapter implements InBoundHandler {

    @Override
    public void channelActive(MessageProcessingContext context) {
        context.channelActive();
    }

    @Override
    public void channelInActive(MessageProcessingContext context) {
        context.channelInactive();
    }

    @Override
    public void channelRead(Object message, MessageProcessingContext context) {
        context.channelRead(message);
    }

    @Override
    public void channelExceptionCaught(MessageProcessingContext context, Exception e) {
        context.channelExceptionCaught(e);
    }
}
