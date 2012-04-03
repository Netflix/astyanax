package com.netflix.astyanax.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;

public class StringUtils {
    /**
     * Convert a string from "_" delimited to lower camel case
     * 
     * @param s
     * @return
     */
    public static String toCamelCase(String s) {
        String[] parts = s.split("_");
        StringBuilder sb = new StringBuilder();
        for (String part : parts) {
            if (sb.length() == 0)
                sb.append(part.toLowerCase());
            else
                sb.append(toProperCase(part));
        }
        return sb.toString();
    }

    public static String toProperCase(String s) {
        return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
    }

    public static <T> String joinClassAttributeValues(final T object,
            String name, Class<T> clazz) {
        Field[] fields = clazz.getDeclaredFields();

        StringBuilder sb = new StringBuilder();
        sb.append(name).append("[");
        sb.append(org.apache.commons.lang.StringUtils.join(Collections2
                .transform(
                        // Filter any field that does not start with lower case
                        // (we expect constants to start with upper case)
                        Collections2.filter(Arrays.asList(fields),
                                new Predicate<Field>() {
                                    @Override
                                    public boolean apply(Field field) {
                                        if ((field.getModifiers() & Modifier.STATIC) == Modifier.STATIC)
                                            return false;
                                        return Character.isLowerCase(field
                                                .getName().charAt(0));
                                    }
                                }),
                        // Convert field to "name=value". value=*** on error
                        new Function<Field, String>() {
                            @Override
                            public String apply(Field field) {
                                Object value;
                                try {
                                    value = field.get(object);
                                } catch (Exception e) {
                                    value = "***";
                                }
                                return field.getName() + "=" + value;
                            }
                        }), ","));
        sb.append("]");

        return sb.toString();
    }

    public static <T> String joinClassGettersValues(final T object,
            String name, Class<T> clazz) {
        Method[] methods = clazz.getDeclaredMethods();

        StringBuilder sb = new StringBuilder();
        sb.append(name).append("[");
        sb.append(org.apache.commons.lang.StringUtils.join(Collections2
                .transform(
                        // Filter any field that does not start with lower case
                        // (we expect constants to start with upper case)
                        Collections2.filter(Arrays.asList(methods),
                                new Predicate<Method>() {
                                    @Override
                                    public boolean apply(Method method) {
                                        if ((method.getModifiers() & Modifier.STATIC) == Modifier.STATIC)
                                            return false;
                                        return org.apache.commons.lang.StringUtils
                                                .startsWith(method.getName(),
                                                        "get");
                                    }
                                }),
                        // Convert field to "name=value". value=*** on error
                        new Function<Method, String>() {
                            @Override
                            public String apply(Method method) {
                                Object value;
                                try {
                                    value = method.invoke(object);
                                } catch (Exception e) {
                                    value = "***";
                                }
                                return org.apache.commons.lang.StringUtils
                                        .uncapitalize(org.apache.commons.lang.StringUtils
                                                .substring(method.getName(), 3))
                                        + "=" + value;
                            }
                        }), ","));
        sb.append("]");

        return sb.toString();
    }
}
