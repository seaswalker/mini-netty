package com.github.skywalker.mininetty.handler.codec.decoder;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.github.skywalker.mininetty.context.MessageProcessingContext;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Black-box unit tests for {@link LineBasedDecoder}.
 *
 * <p>The decoder is treated strictly as a black box: only the public
 * constructors and the documented output contract (decoded frames are passed
 * to the next handler as {@code byte[]} with the trailing newline removed)
 * are relied upon. Nothing about the internal implementation is assumed.</p>
 *
 * <p>Buffers are fed exactly the way the runtime feeds decoders: a freshly
 * allocated buffer into which the read bytes were put, so {@code position}
 * equals the number of real bytes while the limit still points at the buffer
 * capacity (the read path never flips).</p>
 *
 * @author skywalker
 */
public class LineBasedDecoderTest {

    private static final int BUFFER_SLACK = 64;

    // ---------------------------------------------------------------- feed

    /** A read buffer filled with {@code data}; {@code position} = data length. */
    private static ByteBuffer read(byte... data) {
        return read(BUFFER_SLACK, data);
    }

    /** A read buffer of the given capacity filled with {@code data}. */
    private static ByteBuffer read(int capacity, byte... data) {
        ByteBuffer buffer = ByteBuffer.allocate(Math.max(capacity, data.length));
        buffer.put(data);
        return buffer;
    }

    private static ByteBuffer read(String data) {
        return read(bytes(data));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** Feeds chunks and returns the context that recorded every decoded frame. */
    private static RecordingContext feed(LineBasedDecoder decoder, ByteBuffer... chunks) {
        RecordingContext context = new RecordingContext();
        for (ByteBuffer chunk : chunks) {
            decoder.channelRead(chunk, context);
        }
        return context;
    }

    /**
     * Asserts that feeding {@code data} into a fresh {@link LineBasedDecoder} throws an
     * {@link IllegalArgumentException} because the frame exceeds {@code maxFrameLength}.
     */
    private static void assertOverflow(LineBasedDecoder decoder, ByteBuffer chunk, String message) {
        try {
            decoder.channelRead(chunk, new RecordingContext());
            throw new AssertionError("expected an IllegalArgumentException, but channelRead returned normally");
        } catch (IllegalArgumentException e) {
            assertEquals(message, e.getMessage());
        }
    }

    private static void assertFrames(RecordingContext context, String... expected) {
        List<Object> frames = context.messages;
        assertEquals("frame count", expected.length, frames.size());
        for (int i = 0; i < expected.length; i++) {
            Object message = frames.get(i);
            assertTrue("frame " + i + " must be a byte[], but was " + message.getClass().getSimpleName(),
                    message instanceof byte[]);
            assertArrayEquals("frame " + i, bytes(expected[i]), (byte[]) message);
        }
        assertTrue("no exception expected, but got " + context.exceptions, context.exceptions.isEmpty());
    }

    /** A fake context that simply records every forwarded message / exception. */
    private static final class RecordingContext extends MessageProcessingContext {

        final List<Object> messages = new ArrayList<>();
        final List<Exception> exceptions = new ArrayList<>();

        RecordingContext() {
            super(null);
        }

        @Override
        public void channelRead(Object message) {
            messages.add(message);
        }

        @Override
        public void forkedChannelRead(List<?> messages) {
            this.messages.addAll(messages);
        }

        @Override
        public void channelExceptionCaught(Exception e) {
            exceptions.add(e);
        }
    }

    // ---------------------------------------------------------------- tests

    @Test
    public void constructorRejectsNonPositiveMaxFrameLength() {
        try {
            new LineBasedDecoder(0);
            throw new AssertionError("0 must be rejected");
        } catch (IllegalArgumentException expected) {
            // expected
        }
        try {
            new LineBasedDecoder(-1);
            throw new AssertionError("a negative maxFrameLength must be rejected");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }

    @Test
    public void defaultMaxFrameLengthAllowsALongerThanDefaultLine() {
        // The no-arg constructor must not impose an absurdly small limit; a line that
        // exceeds any plausible small default (e.g. 8) still decodes fine.
        StringBuilder longLine = new StringBuilder();
        for (int i = 0; i < 2048; i++) {
            longLine.append('x');
        }
        assertFrames(feed(new LineBasedDecoder(), read(longLine.toString() + "\n")), longLine.toString());
    }

    @Test
    public void lineTerminatedByLF() {
        assertFrames(feed(new LineBasedDecoder(), read("hello\n")), "hello");
    }

    @Test
    public void lineTerminatedByCRLF() {
        assertFrames(feed(new LineBasedDecoder(), read("hello\r\n")), "hello");
    }

    @Test
    public void multipleLinesInOneBufferWithMixedTerminators() {
        assertFrames(feed(new LineBasedDecoder(), read("first\nsecond\r\nthird\nfourth\r\n")),
                "first", "second", "third", "fourth");
    }

    @Test
    public void emptyLineTerminatedByLF() {
        assertFrames(feed(new LineBasedDecoder(), read("\n")), "");
    }

    @Test
    public void emptyLineTerminatedByCRLF() {
        assertFrames(feed(new LineBasedDecoder(), read("\r\n")), "");
    }

    @Test
    public void consecutiveDelimitersProduceEmptyLines() {
        assertFrames(feed(new LineBasedDecoder(), read("a\n\nb\r\n\r\nc\n")), "a", "", "b", "", "c");
    }

    @Test
    public void terminatorAtTheVeryBeginning() {
        assertFrames(feed(new LineBasedDecoder(), read("\nhello\n")), "", "hello");
        assertFrames(feed(new LineBasedDecoder(), read("\r\nhello\r\n")), "", "hello");
    }

    @Test
    public void halfPacketSplitAcrossReadsIsBufferedUntilTheTerminator() {
        assertFrames(feed(new LineBasedDecoder(), read("Hel"), read("lo"), read(" World\n")),
                "Hello World");
    }

    @Test
    public void crlfDelimiterSpanningTwoReads() {
        assertFrames(feed(new LineBasedDecoder(), read("line\r"), read("\n")), "line");
    }

    @Test
    public void loneCarriageReturnIsKeptAsContent() {
        // \r followed by something other than \n is not a terminator and must be preserved.
        assertFrames(feed(new LineBasedDecoder(), read("a\rb\n")), "a\rb");
        assertFrames(feed(new LineBasedDecoder(), read("a\r\r\n")), "a\r");
    }

    @Test
    public void trailingCarriageReturnIsKeptWhenNextReadDoesNotStartWithLF() {
        assertFrames(feed(new LineBasedDecoder(), read("a\r"), read("b\n")), "a\rb");
    }

    @Test
    public void unterminatedDataIsNotEmittedAndIsRetainedAcrossReads() {
        // One connection == one context; every read event of the same connection
        // must be delivered to the same context.
        LineBasedDecoder decoder = new LineBasedDecoder();
        RecordingContext context = new RecordingContext();
        decoder.channelRead(read("abc"), context);
        assertEquals(0, context.messages.size());
        decoder.channelRead(read("def\n"), context);
        assertFrames(context, "abcdef");
    }

    @Test
    public void oneBytePerRead() {
        LineBasedDecoder decoder = new LineBasedDecoder();
        RecordingContext context = new RecordingContext();
        for (byte b : bytes("ping\n")) {
            decoder.channelRead(read(b), context);
        }
        assertFrames(context, "ping");
    }

    @Test
    public void trailingCapacityInTheReadBufferIsIgnored() {
        // Simulates a partial socket read: only the written prefix is real data,
        // the rest of the buffer is uninitialized capacity.
        LineBasedDecoder decoder = new LineBasedDecoder();
        byte[] data = bytes("hello\n");
        ByteBuffer partial = ByteBuffer.allocate(256);
        partial.put(data);
        assertEquals(data.length, partial.position());
        RecordingContext context = feed(decoder, partial);
        assertFrames(context, "hello");
    }

    @Test
    public void emptyReadProducesNoFrameAndKeepsState() {
        // A read of zero real bytes (an empty buffer) must emit nothing and must
        // not disturb already-buffered partial data.
        LineBasedDecoder decoder = new LineBasedDecoder();
        RecordingContext context = new RecordingContext();
        decoder.channelRead(read("abc"), context); // partial, no newline yet
        decoder.channelRead(read(), context);      // 0-byte read event
        assertEquals(0, context.messages.size());
        decoder.channelRead(read("\n"), context);
        assertFrames(context, "abc");
    }

    @Test
    public void eachLineMayFillTheMaxFrameLengthExactly() {
        String line = "abcdefghijklmnop"; // 16 bytes == maxFrameLength
        assertFrames(feed(new LineBasedDecoder(16), read(line + "\n")), line);
    }

    @Test
    public void lineLongerThanMaxFrameLengthThrows() {
        assertOverflow(new LineBasedDecoder(8), read("123456789\n"),
                "Frame length is greater than maxFrameLength: 8");
    }

    @Test
    public void frameAccumulatedAcrossReadsMayTriggerOverflow() {
        // A line split over several reads only exceeds the limit once the final
        // read brings the terminator; it must still raise an exception.
        LineBasedDecoder decoder = new LineBasedDecoder(4);
        decoder.channelRead(read("ab"), new RecordingContext()); // partial, no newline yet
        assertOverflow(decoder, read("cde\n"), "Frame length is greater than maxFrameLength: 4");
    }

    @Test
    public void earlierLinesStillDeliveredBeforeALaterOverflow() {
        LineBasedDecoder decoder = new LineBasedDecoder(8);
        RecordingContext context = feed(decoder, read("ok\n"));
        assertFrames(context, "ok");
        assertOverflow(decoder, read("123456789\n"), "Frame length is greater than maxFrameLength: 8");
    }

    @Test
    public void manyLinesInALargeChunkAreAllDeliveredInOrder() {
        StringBuilder payload = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            payload.append("line").append(i).append("\n");
        }
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            expected.add("line" + i);
        }
        assertFrames(feed(new LineBasedDecoder(), read(payload.toString())),
                expected.toArray(new String[0]));
    }

    @Test
    public void decoderInstancesDoNotSharePendingState() {
        // A partial line buffered in one connection's decoder must not leak into
        // another connection's decoder (which has its own context).
        LineBasedDecoder first = new LineBasedDecoder();
        LineBasedDecoder second = new LineBasedDecoder();

        RecordingContext firstContext = new RecordingContext();
        first.channelRead(read("abc"), firstContext); // no newline yet
        assertEquals(0, firstContext.messages.size());

        RecordingContext secondContext = new RecordingContext();
        second.channelRead(read("xyz\n"), secondContext);
        assertFrames(secondContext, "xyz");

        first.channelRead(read("\n"), firstContext);
        assertFrames(firstContext, "abc");
    }
}
