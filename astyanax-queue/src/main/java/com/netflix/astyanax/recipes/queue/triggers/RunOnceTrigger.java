/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.astyanax.recipes.queue.triggers;

import java.util.concurrent.TimeUnit;


public class RunOnceTrigger extends AbstractTrigger {
    public static class Builder {
        private RunOnceTrigger trigger = new RunOnceTrigger();
        
        public Builder withDelay(long delay, TimeUnit units) {
            trigger.delay = TimeUnit.MILLISECONDS.convert(delay,  units);
            return this;
        }
        
        public RunOnceTrigger build() {
            if (trigger.delay != null)
                trigger.setTriggerTime(System.currentTimeMillis() + trigger.delay);
            else 
                trigger.setTriggerTime(System.currentTimeMillis());
            return trigger;
        }
    }

    private Long delay;   // In millseconds
    
    @Override
    public Trigger nextTrigger() {
        // There is no next trigger.
        return null;
    }
}
