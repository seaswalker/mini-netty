package com.github.skywalker.mininetty.util;

@FunctionalInterface
public interface ThrowingRunnableTask {

    void run() throws Exception;

}
