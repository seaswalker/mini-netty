package com.github.skywalker.mininetty.handler;

import com.github.skywalker.mininetty.context.MessageProcessingContext;

/**
 * A simple {@link InBoundHandlerAdapter} implementation that prints triggered
 * events and received messages.
 *
 * @author skywalker
 */
public class SimpleInBoundHandler extends InBoundHandlerAdapter {

    @Override
    public void channelActive(MessageProcessingContext context) {
        System.out.println("channel active");
    }

    @Override
    public void channelInActive(MessageProcessingContext context) {
        System.out.println("channel inActive: " + Thread.currentThread().getName());
    }

    @Override
    public void channelRead(Object message, MessageProcessingContext context) {
        // Strip the 4-byte length prefix
        context.channel().writeAndFlush(((String) message).substring(4) + "\n");
    }

}
