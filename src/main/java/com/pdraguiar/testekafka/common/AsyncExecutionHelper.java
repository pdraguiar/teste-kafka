package com.pdraguiar.testekafka.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class AsyncExecutionHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncExecutionHelper.class);

    private static final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(50);

    public static CompletableFuture<Void> runAsync(Runnable runnable, String errorMessage){

        return CompletableFuture.runAsync(runnable, executor).exceptionally(exception -> {

            LOGGER.error(errorMessage, exception);

            return null;
        });
    }
}