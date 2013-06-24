package com.netflix.astyanax.recipes.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;

public class LoggingInvalidMessageHandler implements Function<String, Message> {
    private static final Logger LOG = LoggerFactory.getLogger(LoggingInvalidMessageHandler.class);
    @Override
    public Message apply(String input) {
        LOG.warn("Invalid message: " + input);
        return null;
    }
}
