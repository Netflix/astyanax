/*******************************************************************************
 * Copyright 2011 Netflix
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.test;

import java.util.ArrayList;
import java.util.Random;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class ProbabalisticFunction<T, R> implements Function<T,R> {
    public static class Builder<T, R> {
        private ProbabalisticFunction<T, R> function = new ProbabalisticFunction<T,R>();
        private double counter = 0;

        public Builder<T, R> withProbability(double probability, Function<T, R> func) {
            counter += probability;
            function.functions.add(new Entry<T, R>(counter, func));
            return this;
        }
        
        public Builder<T, R> withDefault(Function<T, R> func) {
            function.defaultFunction = func;
            return this;
        }
        
        public Builder<T, R> withAlways(Runnable func) {
            function.always = func;
            return this;
        }
        
        public Function<T, R> build() {
            return function;
        }
    }
    
    public static class Entry<T, R> {
        Entry(double probability, Function<T,R> function) {
            this.probability = probability;
            this.function = function;
        }
        
        double probability;
        Function<T,R> function;
    }
    
    private ArrayList<Entry<T, R>>  functions = Lists.newArrayList();
    private Function<T,R>           defaultFunction;
    private Runnable                always;
    
    @Override
    public R apply(T arg) {
        always.run();
        
        double p = new Random().nextDouble();
        for (Entry<T,R> entry : functions) {
            if (entry.probability > p) {
                return entry.function.apply(arg);
            }
        }
        
        return defaultFunction.apply(arg);
    }
    
    
}
