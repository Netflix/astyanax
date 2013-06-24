package com.netflix.astyanax.recipes.queue;

import com.google.common.base.Function;

public class SimpleMessageHandlerFactory implements MessageHandlerFactory {

    @SuppressWarnings("unchecked")
    @Override
    public Function<ConsumerMessageContext, Boolean> createInstance(String className) throws Exception {
        return (Function<ConsumerMessageContext, Boolean>)Class.forName(className).newInstance();
    }

}
