package com.netflix.astyanax.thrift;

import com.netflix.astyanax.annotations.Component;

public class MockCompositeType {
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

    public MockCompositeType() {

    }

    public MockCompositeType(String part1, Integer part2, Integer part3,
            boolean boolPart, String utf8StringPart) {
        this.stringPart = part1;
        this.intPart = part2;
        this.intPart2 = part3;
        this.boolPart = boolPart;
        this.utf8StringPart = utf8StringPart;
    }

    public MockCompositeType setStringPart(String part) {
        this.stringPart = part;
        return this;
    }

    public String getStringPart() {
        return this.stringPart;
    }

    public MockCompositeType setIntPart1(int value) {
        this.intPart = value;
        return this;
    }

    public int getIntPart1() {
        return this.intPart;
    }

    public MockCompositeType setIntPart2(int value) {
        this.intPart2 = value;
        return this;
    }

    public int getIntPart2() {
        return this.intPart2;
    }

    public MockCompositeType setBoolPart(boolean boolPart) {
        this.boolPart = boolPart;
        return this;
    }

    public boolean getBoolPart() {
        return this.boolPart;
    }

    public MockCompositeType setUtf8StringPart(String str) {
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