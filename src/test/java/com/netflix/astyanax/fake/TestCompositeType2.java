package com.netflix.astyanax.fake;

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
