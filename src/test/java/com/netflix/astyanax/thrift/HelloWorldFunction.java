package com.netflix.astyanax.thrift;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.netflix.astyanax.recipes.queue.Message;

public class HelloWorldFunction implements Function<Message, Boolean>{
    private final static long startTime = System.currentTimeMillis();
    
    @Override
    public Boolean apply(@Nullable Message input) {
        long offset = System.currentTimeMillis() - startTime;
        
        System.out.println("Hello world (" + offset + ") : " + input);
        return true;
    }

}
