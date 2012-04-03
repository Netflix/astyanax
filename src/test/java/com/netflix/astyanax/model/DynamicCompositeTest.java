package com.netflix.astyanax.model;

import org.junit.Test;

import com.netflix.astyanax.serializers.StringSerializer;

public class DynamicCompositeTest {
    @Test
    public void testComposite() {
        DynamicComposite dc = new DynamicComposite();
        for (char ch = 'A'; ch < 'Z'; ch++) {
            dc.addComponent(Character.toString(ch), StringSerializer.get());
        }

    }
}
