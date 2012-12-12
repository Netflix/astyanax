package com.netflix.astyanax.thrift;

import java.util.Random;

import com.netflix.astyanax.recipes.scheduler.Task;
import com.netflix.astyanax.recipes.scheduler.TaskInfo;

public class HelloWorldTask implements Task {
    private static long firstTime = 0;
    
    @Override
    public void execute(TaskInfo task) {
        if (firstTime == 0) {
            firstTime = System.currentTimeMillis();
        }
        
        System.out.println("Hello " + (System.currentTimeMillis() - firstTime));
        
        if (new Random().nextDouble() > 0.7) {
            throw new RuntimeException("Something is wrong here!!!");
        }
    }
}