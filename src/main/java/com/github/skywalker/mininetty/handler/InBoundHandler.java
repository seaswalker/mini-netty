package com.github.skywalker.mininetty.handler;

import com.github.skywalker.mininetty.context.MessageProcessingContext;

/**
 * A data read event handler.
 *
 * @author skywalker
 *
 */
public interface InBoundHandler extends Handler {

    /**
     * Invoked when data is read.
     *
     * @param context the processing context of the current channel
     */
    void channelRead(Object message, MessageProcessingContext context);

}
