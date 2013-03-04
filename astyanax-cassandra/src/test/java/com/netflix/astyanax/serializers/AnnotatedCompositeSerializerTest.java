package com.netflix.astyanax.serializers;

import com.google.common.base.Strings;
import com.netflix.astyanax.annotations.Component;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA.
 * User: omar
 * Date: 3/4/13
 * Time: 5:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class AnnotatedCompositeSerializerTest {

    @Test
    public void testOverflow() {

        AnnotatedCompositeSerializer<Foo> serializer = new AnnotatedCompositeSerializer<Foo>(Foo.class);

        Foo foo = new Foo();
        foo.bar = Strings.repeat("b", 2000);

        ByteBuffer byteBuffer = serializer.toByteBuffer(foo);
    }

    public static class Foo {

        @Component(ordinal = 0)
        private String bar;

    }

}
