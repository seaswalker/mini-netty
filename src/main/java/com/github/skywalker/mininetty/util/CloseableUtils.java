package com.github.skywalker.mininetty.util;

import java.io.Closeable;
import java.io.IOException;

/**
 * Utilities for {@link java.io.Closeable}.
 *
 * @author skywalker
 */
public class CloseableUtils {

    private CloseableUtils() {
    }

    public static void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException ignore) {
        }
    }

}
