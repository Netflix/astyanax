package com.netflix.astyanax.recipes.queue;

import com.google.common.base.Function;

/**
 * Abstraction for creating message handlers.  Implementations of this class can 
 * be used to tie into dependency injection.
 * @author elandau
 *
 */
public interface MessageHandlerFactory {
    Function<MessageContext, Boolean> createInstance(String className) throws Exception;
}
