package com.github.skywalker.mininetty.handler;

import java.util.Arrays;
import java.util.Objects;

import com.github.skywalker.mininetty.context.MessageProcessingContext;

/**
 * Adds handlers to the {@link HandlerChain} when a client channel is established.
 * <p>This way every client connection gets its own handler instances in the chain;
 * otherwise all connections would share the same instances.</p>
 * 
 * @author skywalker
 *
 */
public abstract class HandlerInitializer extends InBoundHandlerAdapter {

    @Override
    public void channelActive(MessageProcessingContext context) {
        Handler[] handlers = init();
        Objects.requireNonNull(handlers);
        context.replaceHandlers(this, Arrays.asList(handlers));
        context.channelActive();
    }

    /**
     * Returns the handlers to add.
     *
     * @return {@link Handler} array
     */
    public abstract Handler[] init();

}
