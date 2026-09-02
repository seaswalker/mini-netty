package com.github.skywalker.mininetty.handler;

import com.github.skywalker.mininetty.context.MessageProcessingContext;

/**
 * A data output (write) event handler.
 *
 * @author skywalker
 */
public interface OutBoundHandler extends Handler {

    /**
     * Writes data back to the client.
     *
     * @param message the message to write
     * @param context the processing context of the current channel
     */
    void channelWrite(Object message, MessageProcessingContext context);

}
