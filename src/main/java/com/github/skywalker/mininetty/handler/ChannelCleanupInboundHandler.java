package com.github.skywalker.mininetty.handler;

import com.github.skywalker.mininetty.context.MessageProcessingContext;

public class ChannelCleanupInboundHandler extends InBoundHandlerAdapter {

    @Override
    public void channelInActive(MessageProcessingContext context) {
        context.channel().close();
        context.channelInactive();
    }

}
