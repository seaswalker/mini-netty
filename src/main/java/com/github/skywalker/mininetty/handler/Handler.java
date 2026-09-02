package com.github.skywalker.mininetty.handler;

import com.github.skywalker.mininetty.context.MessageProcessingContext;

/**
 * An event handler (marker interface).
 *
 * @author skywalker
 *
 */
public interface Handler {

    /**
     * The channel has been established.
     *
     * @param context the processing context of the current channel
     */
    void channelActive(MessageProcessingContext context);

    /**
     * The channel has been closed.
     *
     * @param context the processing context of the current channel
     */
    void channelInActive(MessageProcessingContext context);

    void channelExceptionCaught(MessageProcessingContext context, Exception e);

}
