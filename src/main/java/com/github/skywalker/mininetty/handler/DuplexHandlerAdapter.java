package com.github.skywalker.mininetty.handler;

import com.github.skywalker.mininetty.context.MessageProcessingContext;

public class DuplexHandlerAdapter implements InBoundHandler, OutBoundHandler {

    @Override
    public void channelRead(Object message, MessageProcessingContext context) {
        context.channelRead(message);
    }

    @Override
    public void channelWrite(Object message, MessageProcessingContext context) {
        context.channelWrite(message);
    }

    @Override
    public void channelActive(MessageProcessingContext context) {
        context.channelActive();
    }

    @Override
    public void channelInActive(MessageProcessingContext context) {
        context.channelInactive();
    }

    @Override
    public void channelExceptionCaught(MessageProcessingContext context, Exception e) {
        context.channelExceptionCaught(e);
    }
}
