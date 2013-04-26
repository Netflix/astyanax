package com.netflix.astyanax.thrift;

import java.util.Comparator;

import com.netflix.astyanax.annotations.Component;

public class MockCompositeType {
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (boolPart ? 1231 : 1237);
        result = prime * result + ((intPart == null) ? 0 : intPart.hashCode());
        result = prime * result
                + ((intPart2 == null) ? 0 : intPart2.hashCode());
        result = prime * result
                + ((stringPart == null) ? 0 : stringPart.hashCode());
        result = prime * result
                + ((utf8StringPart == null) ? 0 : utf8StringPart.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MockCompositeType other = (MockCompositeType) obj;
        if (boolPart != other.boolPart)
            return false;
        if (intPart == null) {
            if (other.intPart != null)
                return false;
        } else if (!intPart.equals(other.intPart))
            return false;
        if (intPart2 == null) {
            if (other.intPart2 != null)
                return false;
        } else if (!intPart2.equals(other.intPart2))
            return false;
        if (stringPart == null) {
            if (other.stringPart != null)
                return false;
        } else if (!stringPart.equals(other.stringPart))
            return false;
        if (utf8StringPart == null) {
            if (other.utf8StringPart != null)
                return false;
        } else if (!utf8StringPart.equals(other.utf8StringPart))
            return false;
        return true;
    }

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