package com.github.skywalker.mininetty.context;

import com.github.skywalker.mininetty.handler.*;

import java.util.List;

public class MessageProcessingContext {

    private final ChannelHandlerContext channelHandlerContext;

    private HandlerChain.ChainIterator chainIterator;

    public MessageProcessingContext(ChannelHandlerContext channelHandlerContext) {
        this.channelHandlerContext = channelHandlerContext;
    }

    public static MessageProcessingContext copy(MessageProcessingContext source) {
        MessageProcessingContext newContext = new MessageProcessingContext(source.channelHandlerContext);
        newContext.chainIterator = source.chainIterator.copy();
        return newContext;
    }

    public ChannelHandlerContext channel() {
        return channelHandlerContext;
    }

    public void replaceHandlers(Handler replaced, List<Handler> handlers) {
        channelHandlerContext.getHandlerChain().replaceHandlers(replaced, handlers);
    }

    public void channelActive() {
        if (channelHandlerContext.isRunning()) {
            if (chainIterator == null) {
                chainIterator = channelHandlerContext.getHandlerChain().iterate(HandlerChain.IterateMode.BOTH);
            }
            Handler handler = chainIterator.next();
            if (handler != null) {
                callHandlerWithExceptionCaught(() -> handler.channelActive(this));
            }
        }
    }

    public void channelRead(Object message) {
        if (channelHandlerContext.isRunning()) {
            if (chainIterator == null) {
                chainIterator = channelHandlerContext.getHandlerChain().iterate(HandlerChain.IterateMode.IN);
            }
            Handler handler = chainIterator.next();
            if (handler != null) {
                callHandlerWithExceptionCaught(() -> {
                    InBoundHandler inBoundHandler = (InBoundHandler) handler;
                    inBoundHandler.channelRead(message, this);
                });
            }
        }
    }

    public void forkedChannelRead(List<?> messages) {
        if (!channel().isRunning() || messages == null || messages.isEmpty()) {
            return;
        }

        if (messages.size() == 1) {
            channelRead(messages.get(0));
            return;
        }

        for (Object message : messages) {
            MessageProcessingContext forked = copy(this);
            forked.channelRead(message);
        }
    }

    public void channelWrite(Object message) {
        if (channelHandlerContext.isRunning()) {
            if (chainIterator == null) {
                chainIterator = channelHandlerContext.getHandlerChain().iterate(HandlerChain.IterateMode.OUT);
            }
            Handler handler = chainIterator.next();
            if (handler != null) {
                callHandlerWithExceptionCaught(() -> {
                    OutBoundHandler outBoundHandler = (OutBoundHandler) handler;
                    outBoundHandler.channelWrite(message, this);
                });
            }
        }
    }

    public void channelInactive() {
        if (channelHandlerContext.isRunning()) {
            if (chainIterator == null) {
                chainIterator = channelHandlerContext.getHandlerChain().iterate(HandlerChain.IterateMode.BOTH);
            }
            Handler handler = chainIterator.next();
            if (handler != null) {
                callHandlerWithExceptionCaught(() -> handler.channelInActive(this));
            }
        }
    }

    public void channelExceptionCaught(Exception e) {
        if (channelHandlerContext.isRunning()) {
            if (chainIterator == null) {
                chainIterator = channelHandlerContext.getHandlerChain().iterate(HandlerChain.IterateMode.BOTH);
            }
            Handler handler = chainIterator.next();
            if (handler != null) {
                handler.channelExceptionCaught(this, e);
            }
        }
    }

    private void callHandlerWithExceptionCaught(Runnable task) {
        try {
            task.run();
        } catch (Exception e) {
            channelHandlerContext.fireExceptionCaught(e);
        }
    }

}
