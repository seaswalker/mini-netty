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
import static org.junit.Assert.fail;

/**
 * Black-box unit tests for {@link DelimiterBasedDecoder}.
 *
 * <p>The decoder is treated strictly as a black box: only the public constructors
 * and the documented output contract (decoded frames are passed to the next handler
 * as {@code byte[]}; the delimiter is not included) are relied upon. Nothing about
 * the internal implementation is assumed.</p>
 *
 * <p>Buffers are fed exactly the way the runtime feeds decoders: a freshly allocated
 * buffer into which the read bytes were put, so {@code position} equals the number of
 * real bytes while the limit still points at the buffer capacity (the read path never
 * flips).</p>
 *
 * @author skywalker
 */
public class DelimiterBasedDecoderTest {

    private static final int BUFFER_SLACK = 64;
    private static final byte SEMICOLON = ';';
    private static final byte COMMA = ',';

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

    private static byte[] raw(int... data) {
        byte[] result = new byte[data.length];
        for (int i = 0; i < data.length; i++) {
            result[i] = (byte) data[i];
        }
        return result;
    }

    private static byte[] concat(byte[]... parts) {
        int total = 0;
        for (byte[] part : parts) {
            total += part.length;
        }
        byte[] result = new byte[total];
        int offset = 0;
        for (byte[] part : parts) {
            System.arraycopy(part, 0, result, offset, part.length);
            offset += part.length;
        }
        return result;
    }

    /** Feeds chunks and returns the context that recorded every decoded frame. */
    private static RecordingContext feed(DelimiterBasedDecoder decoder, ByteBuffer... chunks) {
        RecordingContext context = new RecordingContext();
        for (ByteBuffer chunk : chunks) {
            decoder.channelRead(chunk, context);
        }
        return context;
    }

    /**
     * Asserts that feeding {@code chunk} into {@code decoder} raises an
     * {@link IllegalArgumentException} because a frame exceeds {@code maxFrameLength}.
     */
    private static void assertOverflow(DelimiterBasedDecoder decoder, ByteBuffer chunk, int maxFrameLength) {
        try {
            decoder.channelRead(chunk, new RecordingContext());
            fail("expected an IllegalArgumentException for a frame exceeding maxFrameLength, but channelRead returned normally");
        } catch (IllegalArgumentException e) {
            assertEquals("Frame length is greater than maxFrameLength: " + maxFrameLength, e.getMessage());
        }
    }

    private static void assertFrames(RecordingContext context, String... expected) {
        byte[][] expectedBytes = new byte[expected.length][];
        for (int i = 0; i < expected.length; i++) {
            expectedBytes[i] = bytes(expected[i]);
        }
        assertByteFrames(context, expectedBytes);
    }

    private static void assertByteFrames(RecordingContext context, byte[]... expected) {
        List<Object> frames = context.messages;
        assertEquals("frame count", expected.length, frames.size());
        for (int i = 0; i < expected.length; i++) {
            Object message = frames.get(i);
            assertTrue("frame " + i + " must be a byte[], but was " + message.getClass().getSimpleName(),
                    message instanceof byte[]);
            assertArrayEquals("frame " + i, expected[i], (byte[]) message);
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

    // ---------------------------------------------------------------- constructor

    @Test
    public void constructorRejectsNonPositiveMaxFrameLength() {
        for (int bad : new int[] { 0, -1 }) {
            try {
                new DelimiterBasedDecoder(SEMICOLON, bad);
                fail("maxFrameLength " + bad + " must be rejected");
            } catch (IllegalArgumentException expected) {
                assertEquals("maxFrameLength must be greater than 0", expected.getMessage());
            }
        }
    }

    @Test
    public void anyDelimiterByteValueIsAccepted() {
        // A delimiter is just a byte: signed, unsigned, NUL, whitespace - none may throw.
        int[] delimiters = { 0, 1, ' ', ';', 127, 128, 255, 254, -1 };
        for (int delimiter : delimiters) {
            new DelimiterBasedDecoder((byte) delimiter);
            new DelimiterBasedDecoder((byte) delimiter, 16);
        }
    }

    // ---------------------------------------------------------------- basic framing

    @Test
    public void singleFrameTerminated() {
        assertFrames(feed(new DelimiterBasedDecoder(SEMICOLON), read("hello;")), "hello");
    }

    @Test
    public void multipleFramesTerminatedInOneRead() {
        assertFrames(feed(new DelimiterBasedDecoder(SEMICOLON), read("a;b;c;")), "a", "b", "c");
    }

    @Test
    public void contentMayContainAnyNonDelimiterByte() {
        assertFrames(feed(new DelimiterBasedDecoder(COMMA), read("a1 ,b 2,c3,")), "a1 ", "b 2", "c3");
    }

    @Test
    public void emptyFramesFromConsecutiveDelimiters() {
        assertFrames(feed(new DelimiterBasedDecoder(SEMICOLON), read("a;;b;;")), "a", "", "b", "");
    }

    @Test
    public void delimiterAtTheVeryBeginning() {
        assertFrames(feed(new DelimiterBasedDecoder(SEMICOLON), read(";abc;")), "", "abc");
    }

    @Test
    public void trailingEmptyFrameExactlyAtReadEnd() {
        assertFrames(feed(new DelimiterBasedDecoder(SEMICOLON), read("a;")), "a");
        assertFrames(feed(new DelimiterBasedDecoder(SEMICOLON), read("a;b;")), "a", "b");
    }

    @Test
    public void dataWithoutDelimiterIsNotEmitted() {
        RecordingContext context = feed(new DelimiterBasedDecoder(SEMICOLON), read("abc"));
        assertFrames(context);
    }

    @Test
    public void unterminatedPrefixMergesWithNextRead() {
        DelimiterBasedDecoder decoder = new DelimiterBasedDecoder(SEMICOLON);
        RecordingContext context = new RecordingContext();
        decoder.channelRead(read("abc"), context);
        assertFrames(context);
        decoder.channelRead(read("def;"), context);
        assertFrames(context, "abcdef");
    }

    @Test
    public void chunkEndingWithDelimiterResetsCleanlyForNextRead() {
        assertFrames(feed(new DelimiterBasedDecoder(SEMICOLON), read("ab;"), read("cd;")), "ab", "cd");
        // The next read may start directly with the delimiter (empty frame).
        assertFrames(feed(new DelimiterBasedDecoder(SEMICOLON), read("a;"), read(";b;")), "a", "", "b");
        // Or the next read may continue a partial frame that was pending.
        assertFrames(feed(new DelimiterBasedDecoder(SEMICOLON), read("a;b"), read("c;d;")), "a", "bc", "d");
    }

    @Test
    public void delimiterCarriedOverToTheSecondRead() {
        // Frame "abc", delimiter at the very start of the second chunk.
        assertFrames(feed(new DelimiterBasedDecoder(SEMICOLON), read("abc"), read(";def;")), "abc", "def");
    }

    @Test
    public void frameContinuedOverThreeChunks() {
        assertFrames(feed(new DelimiterBasedDecoder(SEMICOLON), read("Hel"), read("lo "), read("World;")),
                "Hello World");
    }

    @Test
    public void oneBytePerReadReassemblesFullFrame() {
        DelimiterBasedDecoder decoder = new DelimiterBasedDecoder(SEMICOLON);
        RecordingContext context = new RecordingContext();
        for (byte b : bytes("ping;")) {
            decoder.channelRead(read(b), context);
        }
        assertFrames(context, "ping");
    }

    @Test
    public void emptyReadProducesNoFrameAndKeepsState() {
        DelimiterBasedDecoder decoder = new DelimiterBasedDecoder(SEMICOLON);
        RecordingContext context = new RecordingContext();
        decoder.channelRead(read("abc"), context); // partial, no delimiter yet
        decoder.channelRead(read(), context);      // 0-byte read event
        assertEquals(0, context.messages.size());
        decoder.channelRead(read("def;"), context);
        assertFrames(context, "abcdef");
    }

    @Test
    public void manyReadsOfEmptyBuffersAreHarmless() {
        DelimiterBasedDecoder decoder = new DelimiterBasedDecoder(SEMICOLON);
        RecordingContext context = new RecordingContext();
        for (int i = 0; i < 10; i++) {
            decoder.channelRead(read(), context);
        }
        assertFrames(context);
    }

    @Test
    public void frameRecoveredExactlyOnceForEveryTwoWaySplit() {
        // Exhaustively feeds the payload + delimiter as two reads, cutting at every
        // possible byte boundary. The frame must be recovered exactly once, with no
        // loss, duplication or corruption - regardless of where the read boundary falls.
        String payload = "Hello World";
        byte[] delimiter = { SEMICOLON };
        for (int cut = 0; cut <= payload.length(); cut++) {
            byte[] first = bytes(payload.substring(0, cut));
            byte[] second = concat(bytes(payload.substring(cut)), delimiter);
            RecordingContext context = feed(new DelimiterBasedDecoder(SEMICOLON),
                    read(first), read(second));
            assertEquals("cut at " + cut, 1, context.messages.size());
            assertByteFrames(context, bytes(payload));
        }
    }

    @Test
    public void arbitraryFragmentationPreservesEveryFrame() {
        // One decoder, one logical stream: the payload (100 frames) is chopped into
        // chunks of varying sizes. Every byte must survive the reassembly.
        StringBuilder stream = new StringBuilder();
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            stream.append("frame-").append(i).append(';');
            expected.add("frame-" + i);
        }
        byte[] payload = bytes(stream.toString());

        int[] step = { 3, 1, 5, 2, 4, 7, 1, 6 };
        DelimiterBasedDecoder decoder = new DelimiterBasedDecoder(SEMICOLON);
        RecordingContext context = new RecordingContext();
        int offset = 0, i = 0;
        while (offset < payload.length) {
            int size = Math.min(step[i++ % step.length], payload.length - offset);
            byte[] chunk = new byte[size];
            System.arraycopy(payload, offset, chunk, 0, size);
            decoder.channelRead(read(chunk), context);
            offset += size;
        }
        assertFrames(context, expected.toArray(new String[0]));
    }

    @Test
    public void stateIsStableAcrossManySequentialFrameGroups() {
        // Simulates a long-lived session on one connection: many frames arriving as
        // arbitrary read groups, including groups that end mid-frame. Cumulative
        // state must never bleed from one group into the next.
        DelimiterBasedDecoder decoder = new DelimiterBasedDecoder(SEMICOLON);
        RecordingContext context = new RecordingContext();

        decoder.channelRead(read("one;"), context);       // group 1: complete
        decoder.channelRead(read("two"), context);        // group 2: partial, kept
        decoder.channelRead(read("-a;t"), context);       // group 3: one complete + partial tail
        decoder.channelRead(read("hree;"), context);      // group 4: completes the tail
        decoder.channelRead(read(";four;"), context);     // group 5: empty + complete

        assertFrames(context, "one", "two-a", "three", "", "four");
    }

    @Test
    public void manyFramesInOneLargeChunkAreAllDeliveredInOrder() {
        StringBuilder payload = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            payload.append("line").append(i).append(';');
        }
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            expected.add("line" + i);
        }
        assertFrames(feed(new DelimiterBasedDecoder(SEMICOLON), read(payload.toString())),
                expected.toArray(new String[0]));
    }

    @Test
    public void decoderInstancesDoNotSharePendingState() {
        DelimiterBasedDecoder first = new DelimiterBasedDecoder(SEMICOLON);
        DelimiterBasedDecoder second = new DelimiterBasedDecoder(SEMICOLON);

        RecordingContext firstContext = new RecordingContext();
        first.channelRead(read("abc"), firstContext); // no delimiter yet
        assertFrames(firstContext);

        RecordingContext secondContext = new RecordingContext();
        second.channelRead(read("xyz;"), secondContext);
        assertFrames(secondContext, "xyz");

        first.channelRead(read(";"), firstContext);
        assertFrames(firstContext, "abc");
    }

    @Test
    public void trailingCapacityInTheReadBufferIsIgnored() {
        byte[] data = bytes("hello;");
        ByteBuffer partial = ByteBuffer.allocate(256);
        partial.put(data);
        assertEquals(data.length, partial.position());
        assertFrames(feed(new DelimiterBasedDecoder(SEMICOLON), partial), "hello");
    }

    // ---------------------------------------------------------------- binary content

    @Test
    public void binaryContentWithSignedDelimiterByte() {
        // A byte-delimiter of 0xFF must only split on the exact byte 0xFF and must
        // preserve every other byte verbatim, including bytes whose signed value
        // would wrap around it.
        DelimiterBasedDecoder decoder = new DelimiterBasedDecoder((byte) 0xFF);
        assertByteFrames(feed(decoder, read(raw(0x00, 0x01, 0xFE, 0xFF))), raw(0x00, 0x01, 0xFE));
    }

    @Test
    public void nulByteAsDelimiter() {
        DelimiterBasedDecoder decoder = new DelimiterBasedDecoder((byte) 0);
        assertByteFrames(feed(decoder, read(raw('a', 0, 'b', 0))), raw('a'), raw('b'));
    }

    @Test
    public void contentSpanningFullSignedByteRange() {
        // Frames contain bytes from both halves of the signed range; only the ASCII
        // delimiter splits. Bytes equal to the delimiter inside content must not exist
        // by construction, and everything else is preserved exactly.
        byte[] frame1 = raw(0x00, 0x7F, 0x80, 0xFF);
        byte[] frame2 = raw(0x01, 0x7E, 0x81, 0xFE);
        DelimiterBasedDecoder decoder = new DelimiterBasedDecoder(COMMA);
        assertByteFrames(feed(decoder, read(concat(frame1, raw(','), frame2, raw(',')))),
                frame1, frame2);
    }

    // ---------------------------------------------------------------- max frame length

    @Test
    public void defaultMaxFrameLengthAllowsAFrameLongerThanAnyTrivialDefault() {
        // The no-arg (default-length) configuration must not impose an absurdly
        // small limit; a frame longer than any plausible tiny default decodes fine.
        StringBuilder longFrame = new StringBuilder();
        for (int i = 0; i < 2048; i++) {
            longFrame.append('x');
        }
        assertFrames(feed(new DelimiterBasedDecoder(SEMICOLON), read(longFrame.toString() + ';')),
                longFrame.toString());
    }

    @Test
    public void frameExactlyFillingMaxFrameLengthIsDelivered() {
        String frame = "abcdefghijklmnop"; // 16 bytes == maxFrameLength
        assertFrames(feed(new DelimiterBasedDecoder(SEMICOLON, 16), read(frame + ';')), frame);
    }

    @Test
    public void frameExactlyFillingMaxFrameLengthAccumulatedAcrossReadsIsDelivered() {
        DelimiterBasedDecoder decoder = new DelimiterBasedDecoder(SEMICOLON, 8);
        RecordingContext context = new RecordingContext();
        decoder.channelRead(read("1234"), context);          // partial
        decoder.channelRead(read("5678"), context);          // fills exactly, still partial
        assertEquals(0, context.messages.size());
        decoder.channelRead(read(";"), context);             // delimiter closes the 8-byte frame
        assertFrames(context, "12345678");
    }

    @Test
    public void frameLongerThanMaxFrameLengthRaises() {
        assertOverflow(new DelimiterBasedDecoder(SEMICOLON, 8), read("123456789;"), 8);
    }

    @Test
    public void accumulatedOverflowAcrossReadsRaises() {
        DelimiterBasedDecoder decoder = new DelimiterBasedDecoder(SEMICOLON, 4);
        decoder.channelRead(read("ab"), new RecordingContext()); // partial, no delimiter yet
        assertOverflow(decoder, read("cde;"), 4);
    }

    @Test
    public void pendingDataAloneMayExceedMaxFrameLength() {
        // A read that never reaches the delimiter can still overflow once the
        // accumulated bytes themselves exceed the cap.
        DelimiterBasedDecoder decoder = new DelimiterBasedDecoder(SEMICOLON, 4);
        decoder.channelRead(read("ab"), new RecordingContext());
        assertOverflow(decoder, read("cde"), 4);
    }

    @Test
    public void earlierCompleteFramesStillDeliveredBeforeALaterOverflow() {
        DelimiterBasedDecoder decoder = new DelimiterBasedDecoder(SEMICOLON, 8);
        RecordingContext context = feed(decoder, read("ok;"));
        assertFrames(context, "ok");
        assertOverflow(decoder, read("123456789;"), 8);
    }

    @Test
    public void overflowDoesNotCorruptAReusableDecoder() {
        // After an overflow has been reported, the decoder must remain usable for
        // a subsequent well-formed frame instead of leaking the failed frame.
        DelimiterBasedDecoder decoder = new DelimiterBasedDecoder(SEMICOLON, 8);
        RecordingContext context = new RecordingContext();
        try {
            decoder.channelRead(read("123456789;"), context);
            fail("expected an IllegalArgumentException for a frame exceeding maxFrameLength");
        } catch (IllegalArgumentException expected) {
            // expected
        }
        decoder.channelRead(read("fine;"), context);
        assertFrames(context, "fine");
    }

    // ---------------------------------------------------------------- pass-through

    @Test
    public void nonByteBufferMessageIsPassedThroughUnchanged() {
        DelimiterBasedDecoder decoder = new DelimiterBasedDecoder(SEMICOLON);
        RecordingContext context = new RecordingContext();
        decoder.channelRead("plain string", context);
        assertEquals(1, context.messages.size());
        assertEquals("plain string", context.messages.get(0));
    }
}
