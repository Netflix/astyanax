package com.netflix.astyanax.thrift;

import java.util.Random;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.netflix.astyanax.recipes.queue.MessageContext;

public class HelloWorldFunction implements Function<MessageContext, Boolean>{
    private final static long startTime = System.currentTimeMillis();
    
    @Override
    public Boolean apply(@Nullable MessageContext input) {
        long offset = System.currentTimeMillis() - startTime;
        
        System.out.println("Hello world (" + offset + ") : " + input);
//        if (new Random().nextDouble() > 0) {
//            throw new RuntimeException("WTF!?");
//        }
        return true;
    }

}
