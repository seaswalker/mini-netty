package com.github.skywalker.mininetty.util;

/**
 * Data utility methods.
 *
 * @author skywalker
 */
public class DataUtils {

    private DataUtils() {}

    /**
     * Converts an int value to a 4-byte array (little-endian).
     */
    public static byte[] int2Bytes(int i) {
        byte[] arr = new byte[4];
        arr[0] = (byte) (i & 0xff);
        arr[1] = (byte) ((i >> 8) & 0xff);
        arr[2] = (byte) ((i >> 16) & 0xff);
        arr[3] = (byte) ((i >> 24) & 0xff);
        return arr;
    }

}
