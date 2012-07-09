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

public class TestCompositeType {
    @Component
    private String stringPart;

    @Component
    private Integer intPart;

    @Component
    private Integer intPart2;

    @Component
    private boolean boolPart;

    @Component
    private String utf8StringPart;

    public TestCompositeType() {

    }

    public TestCompositeType(String part1, Integer part2, Integer part3,
            boolean boolPart, String utf8StringPart) {
        this.stringPart = part1;
        this.intPart = part2;
        this.intPart2 = part3;
        this.boolPart = boolPart;
        this.utf8StringPart = utf8StringPart;
    }

    public TestCompositeType setStringPart(String part) {
        this.stringPart = part;
        return this;
    }

    public String getStringPart() {
        return this.stringPart;
    }

    public TestCompositeType setIntPart1(int value) {
        this.intPart = value;
        return this;
    }

    public int getIntPart1() {
        return this.intPart;
    }

    public TestCompositeType setIntPart2(int value) {
        this.intPart2 = value;
        return this;
    }

    public int getIntPart2() {
        return this.intPart2;
    }

    public TestCompositeType setBoolPart(boolean boolPart) {
        this.boolPart = boolPart;
        return this;
    }

    public boolean getBoolPart() {
        return this.boolPart;
    }

    public TestCompositeType setUtf8StringPart(String str) {
        this.utf8StringPart = str;
        return this;
    }

    public String getUtf8StringPart() {
        return this.utf8StringPart;
    }

    public String toString() {
        return new StringBuilder().append("MockCompositeType[")
                .append(stringPart).append(',').append(intPart).append(',')
                .append(intPart2).append(',').append(boolPart).append(',')
                .append(utf8StringPart).append(']').toString();
    }
}
