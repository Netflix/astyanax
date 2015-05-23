package com.netflix.astyanax.serializers;

import com.google.common.base.Strings;
import com.netflix.astyanax.annotations.Component;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Date;

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
        foo.bar1 = Strings.repeat("b", 2000);
        foo.bar2 = Strings.repeat("b", 4192);

        ByteBuffer byteBuffer = serializer.toByteBuffer(foo);
    }

    @Test
    public void testOverflow2() {
        AnnotatedCompositeSerializer<Foo2> serializer = new AnnotatedCompositeSerializer<Foo2>(
                Foo2.class);

        Foo2 foo = new Foo2();
        foo.bar  = Strings.repeat("b", 500);
        foo.test = Strings.repeat("b", 12);

        ByteBuffer byteBuffer = serializer.toByteBuffer(foo);
    }
    
    public static class Foo2 {
        @Component(ordinal = 0)
        private Date updateTimestamp;

        @Component(ordinal = 1)
        private String bar;

        @Component(ordinal = 2)
        private String test;
    }

    public static class Foo {

        @Component(ordinal = 0)
        private String bar;

        @Component(ordinal = 0)
        private String bar1;

        @Component(ordinal = 0)
        private String bar2;

    }

}
