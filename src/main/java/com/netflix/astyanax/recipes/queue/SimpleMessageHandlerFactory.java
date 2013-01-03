package com.netflix.astyanax.recipes.queue;

import com.google.common.base.Function;

public class SimpleMessageHandlerFactory implements MessageHandlerFactory {

    @Override
    public Function<MessageContext, Boolean> createInstance(String className) throws Exception {
        return (Function<MessageContext, Boolean>)Class.forName(className).newInstance();
    }

}
