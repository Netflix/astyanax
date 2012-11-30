package com.netflix.astyanax.thrift;

import java.util.Random;

import com.netflix.astyanax.recipes.scheduler.Task;
import com.netflix.astyanax.recipes.scheduler.TaskInfo;

public class HelloWorldTask implements Task {
    @Override
    public void execute(TaskInfo task) {
        if (new Random().nextDouble() > 0.7) {
            throw new RuntimeException("Something is wrong here!!!");
        }
        System.out.println("Hello " + System.currentTimeMillis());
    }
}