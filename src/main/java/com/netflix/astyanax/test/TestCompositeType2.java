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

import com.netflix.astyanax.annotations.Component;

public class TestCompositeType2 {
    @Component
    private boolean part1;

    @Component
    private String part2;

    public TestCompositeType2() {

    }

    public TestCompositeType2(boolean part1, String part2) {
        this.part1 = part1;
        this.part2 = part2;
    }

    public String toString() {
        return new StringBuilder().append("MockCompositeType2(").append(part1)
                .append(",").append(part2).append(")").toString();
    }

}
