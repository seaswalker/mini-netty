package com.github.skywalker.mininetty.handler;

import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.List;

/**
 * {@link Handler} chain.
 *
 * @author skywalker
 */
public class HandlerChain {

    private final DefaultOutBoundHandler defaultOutBoundHandler = new DefaultOutBoundHandler();
    private final Node head = new Node(null);
    private final Node tail = new Node(null);

    /**
     * Appends handlers to the chain, after the built-in handlers.
     */
    public void addHandlers(List<Handler> handlers) {
        List<Handler> builtInHandlers = Arrays.asList(
                new LoggingOutboundHandler(),
                new ChannelCleanupInboundHandler(),
                new IdleDetectionDuplexHandler(),
                defaultOutBoundHandler
        );
        Chain builtInChain = buildChainFor(builtInHandlers);

        head.next = builtInChain.head;
        builtInChain.head.prev = head;

        Node prev = builtInChain.tail;
        for (Handler handler : handlers) {
            Node node = new Node(handler);
            prev.next = node;
            node.prev = prev;
            prev = node;
        }

        tail.prev = prev;
        prev.next = tail;
    }

    public void replaceHandlers(Handler replaced, List<Handler> newHandlers) {
        Node node = head.next;
        while (node != tail) {
            if (node.handler == replaced) {
                replaceHandlersAt(node, newHandlers);
                break;
            }
            node = node.next;
        }
    }

    public DefaultOutBoundHandler getDefaultOutBoundHandler() {
        return defaultOutBoundHandler;
    }

    public boolean isEmpty() {
        return head.next == null;
    }

    public ChainIterator iterate(IterateMode mode) {
        switch (mode) {
            case IN:
                return new InboundChainIterator();
            case OUT:
                return new OutboundChainIterator();
            case BOTH:
                return new CompositeChainIterator(
                        new ChainIterator[]{
                                new InboundChainIterator(),
                                new OutboundChainIterator()
                        });
        }
        throw new IllegalArgumentException("Unknown iterate mode " + mode);
    }

    public interface ChainIterator {
        @Nullable
        Handler next();

        ChainIterator copy();
    }

    public enum IterateMode {
        IN,
        OUT,
        BOTH
    }

    private void replaceHandlersAt(Node node, List<Handler> handlers) {
        Chain chain = buildChainFor(handlers);
        Node prev = node.prev;
        Node next = node.next;

        prev.next = chain.head;
        chain.head.prev = prev;

        chain.tail.next = next;
        next.prev = chain.tail;

        node.detach();
    }

    private Chain buildChainFor(List<Handler> handlers) {
        Node prev = null;
        Node head = null;
        Node node = null;

        for (Handler handler : handlers) {
            node = new Node(handler);
            if (prev == null) {
                head = node;
            } else {
                prev.next = node;
                node.prev = prev;
            }
            prev = node;
        }

        return new Chain(head, node);
    }

    private class InboundChainIterator implements ChainIterator {
        Node current = head;

        @Override
        public @Nullable Handler next() {
            Node candidate;
            while ((candidate = current.next) != tail) {
                if (candidate.handler instanceof InBoundHandler) {
                    if (!(candidate.handler instanceof HandlerInitializer)) {
                        current = candidate;
                    }
                    return candidate.handler;
                }
                current = candidate;
            }
            return null;
        }

        @Override
        public ChainIterator copy() {
            InboundChainIterator iterator = new InboundChainIterator();
            iterator.current = this.current;
            return iterator;
        }
    }

    private class OutboundChainIterator implements ChainIterator {
        Node current = tail;

        @Override
        public @Nullable Handler next() {
            Node candidate;
            while ((candidate = current.prev) != head) {
                if (candidate.handler instanceof OutBoundHandler) {
                    current = candidate;
                    return candidate.handler;
                }
                current = candidate;
            }
            return null;
        }

        @Override
        public ChainIterator copy() {
            OutboundChainIterator iterator = new OutboundChainIterator();
            iterator.current = this.current;
            return iterator;
        }

    }

    private static class CompositeChainIterator implements ChainIterator {
        final ChainIterator[] iterators;
        int index = 0;

        CompositeChainIterator(ChainIterator[] iterators) {
            this.iterators = iterators;
        }

        @Override
        public @Nullable Handler next() {
            if (index >= iterators.length) {
                return null;
            }

            ChainIterator it = iterators[index];
            Handler next = it.next();
            if (next != null) {
                return next;
            }

            ++index;
            return next();
        }

        @Override
        public ChainIterator copy() {
            ChainIterator[] copy = (ChainIterator[]) Arrays.stream(this.iterators).map(ChainIterator::copy).toArray();
            CompositeChainIterator iterator = new CompositeChainIterator(copy);
            iterator.index = index;
            return iterator;
        }

    }

    private static class Node {
        Node prev;
        Node next;
        final Handler handler;

        Node(Handler handler) {
            this.handler = handler;
        }

        void detach() {
            prev = null;
            next = null;
        }
    }

    private static class Chain {
        final Node head;
        final Node tail;

        Chain(Node head, Node tail) {
            this.head = head;
            this.tail = tail;
        }
    }

}
