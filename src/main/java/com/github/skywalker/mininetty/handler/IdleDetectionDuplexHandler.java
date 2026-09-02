package com.github.skywalker.mininetty.handler;

import com.github.skywalker.mininetty.context.MessageProcessingContext;

public class IdleDetectionDuplexHandler extends DuplexHandlerAdapter {

    @Override
    public void channelRead(Object message, MessageProcessingContext context) {
        context.channel().setLastActiveTime(System.nanoTime());
        context.channelRead(message);
    }

    @Override
    public void channelWrite(Object message, MessageProcessingContext context) {
        context.channel().setLastActiveTime(System.nanoTime());
        context.channelWrite(message);
    }

}
