package com.github.skywalker.mininetty.handler.codec.decoder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
 * Black-box unit tests for {@link LengthFieldBasedDecoder}.
 *
 * <p>The decoder is treated strictly as a black box: only the public constructors
 * and the documented output contract (a complete frame is passed to the next handler
 * as one {@code byte[]}) are relied upon. Nothing about the internal state machine is
 * assumed.</p>
 *
 * <p>Frame layout used throughout (verified experimentally, see {@code lengthFieldProbe}):
 * a frame occupies {@code offset + length + contentLength} bytes, where
 * {@code offset} leading bytes are opaque, the following {@code length} bytes hold the
 * length field (unsigned, little-endian by default), and {@code contentLength} equals
 * the value of that field. The emitted {@code byte[]} is the entire frame, i.e. the
 * prefix bytes plus the length field plus the content.</p>
 *
 * <p>Buffers are fed exactly the way the runtime feeds decoders: a freshly allocated
 * buffer into which the read bytes were put, so {@code position} equals the number of
 * real bytes while the limit still points at the buffer capacity (the read path never
 * flips).</p>
 *
 * @author skywalker
 */
public class LengthFieldBasedDecoderTest {

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

    /**
     * Encodes {@code content.length} as an unsigned {@code width}-byte field in the
     * given byte order.
     */
    private static byte[] lengthBytes(int contentLength, int width, ByteOrder order) {
        byte[] field = new byte[width];
        for (int i = 0; i < width; i++) {
            int shift;
            if (order == ByteOrder.BIG_ENDIAN) {
                shift = (width - 1 - i) << 3;
            } else {
                shift = i << 3;
            }
            field[i] = (byte) ((contentLength >>> shift) & 0xff);
        }
        return field;
    }

    /**
     * Builds a complete wire frame: {@code offset} opaque prefix bytes, then the
     * length field of width {@code length} holding {@code content.length}, then the
     * content itself.
     */
    private static byte[] encode(int offset, int length, ByteOrder order, byte[] prefix, byte[] content) {
        if (prefix.length != offset) {
            throw new IllegalArgumentException("prefix length must equal offset");
        }
        return concat(prefix, lengthBytes(content.length, length, order), content);
    }

    private static byte[] encode(int offset, int length, byte[] content) {
        return encode(offset, length, ByteOrder.LITTLE_ENDIAN, fill(offset), content);
    }

    private static byte[] encodeLength(byte[] content, int length) {
        return encode(0, length, content);
    }

    /** {@code count} distinct filler bytes (to catch mis-ordered copies). */
    private static byte[] fill(int count) {
        byte[] result = new byte[count];
        for (int i = 0; i < count; i++) {
            result[i] = (byte) (0x41 + (i % 26));
        }
        return result;
    }

    /** Feeds chunks and returns the context that recorded every decoded frame. */
    private static RecordingContext feed(LengthFieldBasedDecoder decoder, ByteBuffer... chunks) {
        RecordingContext context = new RecordingContext();
        for (ByteBuffer chunk : chunks) {
            decoder.channelRead(chunk, context);
        }
        return context;
    }

    /**
     * Asserts that feeding {@code chunk} into {@code decoder} raises an
     * {@link IllegalStateException} announcing an over-length frame.
     */
    private static void assertOverflow(LengthFieldBasedDecoder decoder, ByteBuffer chunk) {
        try {
            decoder.channelRead(chunk, new RecordingContext());
            fail("expected an IllegalStateException for an over-length frame, but channelRead returned normally");
        } catch (IllegalStateException e) {
            assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("exceeds the max length"));
        }
    }

    private static void assertFrames(RecordingContext context, byte[]... expected) {
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

    // ---------------------------------------------------------------- content: emits the entire frame

    @Test
    public void emitsWholeFrameIncludingLengthField() {
        byte[] content = bytes("hello");
        byte[] frame = encodeLength(content, 4);
        assertFrames(feed(new LengthFieldBasedDecoder(0, 4), read(frame)), frame);
    }

    @Test
    public void lengthFieldWidth1() {
        byte[] content = bytes("hi");
        byte[] frame = encodeLength(content, 1);
        assertFrames(feed(new LengthFieldBasedDecoder(0, 1), read(frame)), frame);
    }

    @Test
    public void lengthFieldWidth2() {
        byte[] content = bytes("netty");
        byte[] frame = encodeLength(content, 2);
        assertFrames(feed(new LengthFieldBasedDecoder(0, 2), read(frame)), frame);
    }

    @Test
    public void lengthFieldWidth3() {
        byte[] content = bytes("wheel");
        byte[] frame = encodeLength(content, 3);
        assertFrames(feed(new LengthFieldBasedDecoder(0, 3), read(frame)), frame);
    }

    @Test
    public void contentLengthZero() {
        byte[] frame = encodeLength(new byte[0], 4);
        assertFrames(feed(new LengthFieldBasedDecoder(0, 4), read(frame)), frame);
    }

    @Test
    public void contentLengthZeroBetweenFrames() {
        byte[] a = encodeLength(bytes("a"), 4);
        byte[] b = encodeLength(new byte[0], 4);
        byte[] c = encodeLength(bytes("zz"), 4);
        assertFrames(feed(new LengthFieldBasedDecoder(0, 4), read(concat(a, b, c))), a, b, c);
    }

    @Test
    public void lengthFieldValueAbove127ReadUnsigned() {
        // 200 content bytes cannot be represented with a signed 1-byte field.
        byte[] content = new byte[200];
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) i;
        }
        byte[] frame = encodeLength(content, 1);
        assertFrames(feed(new LengthFieldBasedDecoder(0, 1), read(frame)), frame);
    }

    @Test
    public void bigContentAcrossMultipleReads() {
        byte[] content = new byte[300];
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) i;
        }
        byte[] frame = encodeLength(content, 2);
        assertFrames(feed(new LengthFieldBasedDecoder(0, 2), read(frame)),
                frame);
    }

    // ---------------------------------------------------------------- offset prefix

    @Test
    public void offsetLargerThanZero() {
        byte[] content = bytes("abc");
        byte[] frame = encode(2, 2, content);
        assertFrames(feed(new LengthFieldBasedDecoder(2, 2), read(frame)), frame);
    }

    @Test
    public void offsetAndWidth4WithDistinctPrefix() {
        byte[] prefix = { 0x11, 0x22, 0x33 };
        byte[] content = bytes("frame");
        byte[] frame = encode(3, 4, ByteOrder.LITTLE_ENDIAN, prefix, content);
        assertFrames(feed(new LengthFieldBasedDecoder(3, 4), read(frame)), frame);
    }

    @Test
    public void frameContentThatLooksLikeAHeaderIsNotParsedAsOne() {
        // The content of frame1 is bytes that, taken at the wrong alignment, would be a
        // perfectly plausible length header. The decoder must consume them as content
        // (length-framed, not scanned) and then decode frame2 normally.
        byte[] frame2 = encodeLength(bytes("payload"), 4); // header = {7,0,0,0}
        byte[] headerLike = slice(frame2, 0, 4);
        byte[] frame1 = encodeLength(headerLike, 4);
        byte[] stream = concat(frame1, frame2);
        assertFrames(feed(new LengthFieldBasedDecoder(0, 4), read(stream)), frame1, frame2);
    }

    // ---------------------------------------------------------------- byte order

    @Test
    public void littleEndianIsTheDefault() {
        byte[] content = bytes("abc");
        byte[] frame = encode(0, 4, ByteOrder.LITTLE_ENDIAN, new byte[0], content);
        assertFrames(feed(new LengthFieldBasedDecoder(0, 4), read(frame)), frame);
    }

    @Test
    public void bigEndianExplicit() {
        byte[] content = bytes("abc");
        byte[] frame = encode(0, 4, ByteOrder.BIG_ENDIAN, new byte[0], content);
        assertFrames(feed(new LengthFieldBasedDecoder(ByteOrder.BIG_ENDIAN, 0, 4), read(frame)), frame);
    }

    @Test
    public void sameBytesDecodedDifferentlyByByteOrder() {
        // Bytes {0x00, 0x01} mean 0x0100 = 256 little-endian but 0x0001 = 1 big-endian.
        byte[] bytes = { 0x00, 0x01, 'x' };
        // BE: length 1 -> frame is {00 01 x}.
        assertFrames(feed(new LengthFieldBasedDecoder(ByteOrder.BIG_ENDIAN, 0, 2),
                read(bytes)), bytes);
        // LE: length 256 -> after the header only 1 content byte has arrived; nothing yet.
        RecordingContext le = new RecordingContext();
        LengthFieldBasedDecoder leDecoder = new LengthFieldBasedDecoder(ByteOrder.LITTLE_ENDIAN, 0, 2);
        leDecoder.channelRead(read(bytes), le);
        assertEquals(0, le.messages.size());
        // The remaining 255 content bytes complete the 258-byte frame.
        byte[] tail = new byte[255];
        leDecoder.channelRead(read(tail), le);
        assertFrames(le, concat(bytes, tail));
    }

    @Test
    public void byteOrderIsHonouredInsideASingleFrame() {
        // A 1-byte length can be read with either order identically, so use width 2.
        byte[] content = bytes("data");
        byte[] be = encode(0, 2, ByteOrder.BIG_ENDIAN, new byte[0], content);
        byte[] le = encode(0, 2, ByteOrder.LITTLE_ENDIAN, new byte[0], content);
        assertFrames(feed(new LengthFieldBasedDecoder(ByteOrder.BIG_ENDIAN, 0, 2), read(be)), be);
        assertFrames(feed(new LengthFieldBasedDecoder(ByteOrder.LITTLE_ENDIAN, 0, 2), read(le)), le);
    }

    // ---------------------------------------------------------------- sticky packets (several frames per read)

    @Test
    public void twoBackToBackFramesInOneRead() {
        byte[] a = encodeLength(bytes("abc"), 4);
        byte[] b = encodeLength(bytes("xy"), 4);
        assertFrames(feed(new LengthFieldBasedDecoder(0, 4), read(concat(a, b))), a, b);
    }

    @Test
    public void manyBackToBackFramesInOneRead() {
        byte[] all = new byte[0];
        byte[][] frames = new byte[10][];
        for (int i = 0; i < 10; i++) {
            frames[i] = encodeLength(bytes("frame-" + i + "-padding"), 4);
            all = concat(all, frames[i]);
        }
        assertFrames(feed(new LengthFieldBasedDecoder(0, 4), read(all)), frames);
    }

    @Test
    public void framesSpanningManyReads() {
        byte[] a = encodeLength(bytes("aaaa"), 4);
        byte[] b = encodeLength(bytes("bbbbbb"), 4);
        byte[] c = encodeLength(bytes("c"), 4);
        byte[] stream = concat(a, b, c);
        // every possible chunk boundary
        for (int cut = 1; cut < stream.length; cut++) {
            assertFrames(feed(new LengthFieldBasedDecoder(0, 4),
                    read(stream, 0, cut), read(stream, cut, stream.length - cut)),
                    a, b, c);
        }
    }

    @Test
    public void zeroAndNonZeroLengthFramesAlternate() {
        byte[] empty = encodeLength(new byte[0], 4);
        byte[] one = encodeLength(bytes("x"), 4);
        byte[] two = encodeLength(bytes("yy"), 4);
        assertFrames(feed(new LengthFieldBasedDecoder(0, 4), read(concat(empty, one, empty, two, empty))),
                empty, one, empty, two, empty);
    }

    // ---------------------------------------------------------------- half packets (a frame split across reads)

    @Test
    public void headerIntactContentSplitAtEveryByte() {
        byte[] content = bytes("hello");
        byte[] frame = encodeLength(content, 4);
        byte[] header = slice(frame, 0, 4); // the real length field announcing 5
        for (int cut = 1; cut < content.length; cut++) {
            ByteBuffer first = read(concat(header, slice(content, 0, cut)));
            ByteBuffer rest = read(slice(content, cut, content.length - cut));
            assertFrames(feed(new LengthFieldBasedDecoder(0, 4), first, rest), frame);
        }
    }

    @Test
    public void contentOneByteAtATime() {
        byte[] content = bytes("hello world");
        byte[] frame = encodeLength(content, 4);
        byte[] header = slice(frame, 0, 4); // the real length field announcing 11
        RecordingContext context = new RecordingContext();
        LengthFieldBasedDecoder decoder = new LengthFieldBasedDecoder(0, 4);
        decoder.channelRead(read(header), context);
        for (int i = 0; i < content.length; i++) {
            decoder.channelRead(read(content[i]), context);
        }
        assertFrames(context, frame);
    }

    @Test
    public void wholeFrameSplitIntoTwoHalfPackets() {
        byte[] frame = encodeLength(bytes("hello, netty wheel!"), 4);
        for (int cut = 1; cut < frame.length; cut++) {
            assertFrames(feed(new LengthFieldBasedDecoder(0, 4),
                    read(frame, 0, cut), read(frame, cut, frame.length - cut)), frame);
        }
    }

    @Test
    public void wholeFrameSplitOneBytePerRead() {
        byte[] frame = encodeLength(bytes("fragmented"), 4);
        RecordingContext context = new RecordingContext();
        LengthFieldBasedDecoder decoder = new LengthFieldBasedDecoder(0, 4);
        for (byte b : frame) {
            decoder.channelRead(read(b), context);
        }
        assertFrames(context, frame);
    }

    @Test
    public void framesSplitArbitrarilyAcrossReads() {
        byte[] a = encodeLength(bytes("alpha"), 4);
        byte[] b = encodeLength(bytes("beta-long"), 4);
        byte[] c = encodeLength(bytes("gamma"), 4);
        byte[] stream = concat(a, b, c);
        // split at every byte position
        for (int cut = 1; cut < stream.length - 1; cut++) {
            assertFrames(feed(new LengthFieldBasedDecoder(0, 4),
                    read(stream, 0, cut), read(stream, cut, stream.length - cut)), a, b, c);
        }
    }

    // ---------------------------------------------------------------- cross-read accumulation

    @Test
    public void partialSecondFrameAccumulatesUntilComplete() {
        byte[] a = encodeLength(bytes("abc"), 4);
        byte[] b = encodeLength(bytes("pqrst"), 4);
        byte[] first = concat(a, slice(b, 0, 3));
        byte[] rest = slice(b, 3, b.length - 3);
        assertFrames(feed(new LengthFieldBasedDecoder(0, 4), read(first), read(rest)), a, b);
    }

    @Test
    public void manyPartialFramesQueueUpAndDrainInOrder() {
        byte[] a = encodeLength(bytes("AAAA"), 4);       // 4 content bytes
        byte[] b = encodeLength(bytes("BBBBBBBB"), 4);   // 8 content bytes
        byte[] c = encodeLength(bytes("CC"), 4);         // 2 content bytes
        byte[] d = encodeLength(bytes("DDDDD"), 4);      // 5 content bytes
        byte[] stream = concat(a, b, c, d);
        // cumulative chunk boundaries that never split a header:
        //  a | b(header+2 content) | b(+6 content) | c | d(header+1 content) | d(+4 content)
        int[] cuts = { 8, 14, 20, 26, 31 };
        RecordingContext context = new RecordingContext();
        LengthFieldBasedDecoder decoder = new LengthFieldBasedDecoder(0, 4);
        int from = 0;
        for (int cut : cuts) {
            decoder.channelRead(read(slice(stream, from, cut - from)), context);
            from = cut;
        }
        decoder.channelRead(read(slice(stream, from, stream.length - from)), context);
        assertFrames(context, a, b, c, d);
    }

    // ---------------------------------------------------------------- empty input

    @Test
    public void emptyReadIsIgnoredAndStateIsPreserved() {
        LengthFieldBasedDecoder decoder = new LengthFieldBasedDecoder(0, 4);
        assertFrames(feed(decoder, read(new byte[0])));
        byte[] frame = encodeLength(bytes("still works"), 4);
        assertFrames(feed(decoder, read(frame)), frame);
    }

    // ---------------------------------------------------------------- non-ByteBuffer passthrough

    @Test
    public void nonByteBufferMessagesPassThrough() {
        LengthFieldBasedDecoder decoder = new LengthFieldBasedDecoder(0, 4);
        RecordingContext context = new RecordingContext();
        Object message = new Object();
        decoder.channelRead(message, context);
        assertEquals("non-ByteBuffer must be forwarded untouched", 1, context.messages.size());
        assertTrue(message == context.messages.get(0));
    }

    // ---------------------------------------------------------------- maxLength

    @Test
    public void maxLengthExactFitIsAccepted() {
        // dataOffset = 4, so the content budget is maxLength - 4 = 4.
        byte[] content = bytes("abcd");
        byte[] frame = encodeLength(content, 4);
        assertFrames(feed(new LengthFieldBasedDecoder(0, 4, 8), read(frame)), frame);
    }

    @Test
    public void maxLengthOneByteOverIsRejected() {
        byte[] frame = encodeLength(bytes("abcde"), 4); // 5 content bytes > budget 4
        assertOverflow(new LengthFieldBasedDecoder(0, 4, 8), read(frame));
    }

    @Test
    public void maxLengthAppliesToTotalFrameNotJustContent() {
        // maxLength 8 == header(4) + content(4): a 4-byte content is the exact fit.
        byte[] ok = encodeLength(bytes("1234"), 4);
        assertFrames(feed(new LengthFieldBasedDecoder(0, 4, 8), read(ok)), ok);
        byte[] bad = encodeLength(bytes("12345"), 4);
        assertOverflow(new LengthFieldBasedDecoder(0, 4, 8), read(bad));
    }

    @Test
    public void maxLengthCountsTheOffsetPrefixToo() {
        // offset=2 + width=2 -> dataOffset=4. maxLength=8 => budget 4 content bytes.
        byte[] prefix = fill(2);
        byte[] frame = encode(2, 2, ByteOrder.LITTLE_ENDIAN, prefix, bytes("1234"));
        assertFrames(feed(new LengthFieldBasedDecoder(2, 2, 8), read(frame)), frame);
        byte[] over = encode(2, 2, ByteOrder.LITTLE_ENDIAN, prefix, bytes("12345"));
        assertOverflow(new LengthFieldBasedDecoder(2, 2, 8), read(over));
    }

    @Test
    public void overLimitAnnouncedBeforeContentArrives() {
        // announce 11 bytes but only 4-byte header present -> must still be rejected.
        byte[] header = lengthBytes(11, 4, ByteOrder.LITTLE_ENDIAN);
        assertOverflow(new LengthFieldBasedDecoder(0, 4, 10), read(header));
    }

    @Test
    public void overflowDuringAccumulatedHeader() {
        // A partial header was already buffered; the completing read announces an
        // over-limit length. The decoder must reject it even though part arrived earlier.
        byte[] header = lengthBytes(11, 4, ByteOrder.LITTLE_ENDIAN);
        LengthFieldBasedDecoder decoder = new LengthFieldBasedDecoder(0, 4, 10);
        try {
            decoder.channelRead(read(header[0]), new RecordingContext());
            decoder.channelRead(read(slice(header, 1, 3)), new RecordingContext());
            fail("expected an IllegalStateException for an over-length frame");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("exceeds the max length"));
        }
    }

    @Test
    public void defaultMaxLengthAllowsBigFrames() {
        // default budget = 2048 - 4 = 2044 content bytes
        byte[] content = new byte[2044];
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) (i * 7);
        }
        byte[] frame = encodeLength(content, 4);
        assertFrames(feed(new LengthFieldBasedDecoder(0, 4), read(frame)), frame);
    }

    // ---------------------------------------------------------------- helpers

    private static byte[] slice(byte[] data, int from, int len) {
        byte[] result = new byte[len];
        System.arraycopy(data, from, result, 0, len);
        return result;
    }

    /** {@code data[from, from+len)} put into a fresh read buffer. */
    private static ByteBuffer read(byte[] data, int from, int len) {
        return read(slice(data, from, len));
    }

}
