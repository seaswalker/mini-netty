package com.github.skywalker.mininetty.handler;

import com.github.skywalker.mininetty.context.MessageProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingOutboundHandler extends OutBoundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(LoggingOutboundHandler.class);

    @Override
    public void channelExceptionCaught(MessageProcessingContext context, Exception e) {
        logger.error("Exception caught in client: {}, close channel.", context.channel().getClientIdentifier(), e);
        context.channel().fireChannelInActive();
    }
}
