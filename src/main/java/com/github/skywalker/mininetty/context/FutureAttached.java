package com.github.skywalker.mininetty.context;

import java.util.concurrent.CompletableFuture;

public interface FutureAttached {

    CompletableFuture<?> getFuture();

}
