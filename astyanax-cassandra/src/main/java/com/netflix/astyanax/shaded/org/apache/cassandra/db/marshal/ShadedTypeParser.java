/*******************************************************************************
 * Copyright 2018 Netflix
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
package com.netflix.astyanax.shaded.org.apache.cassandra.db.marshal;

import com.netflix.astyanax.shaded.org.apache.cassandra.exceptions.ConfigurationException;
import com.netflix.astyanax.shaded.org.apache.cassandra.exceptions.SyntaxException;
import com.netflix.astyanax.shaded.org.apache.cassandra.utils.FBUtilities;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * Extend {@link TypeParser} to support shaded {@link AbstractType} from shaded package.
 *
 * This implementation uses some ugly reflection because {@link TypeParser#parse(String)} was apparently never meant
 * to be overridden -- too many references to private fields (e.g. {@link TypeParser#idx}, etc.) and private methods
 * ({@link TypeParser#getAbstractType}, etc.) which a derived method can't access. This ugliness could have been
 * avoided if TypeParser hadn't tried to restrict extension by setting everything private instead of protected.
 */
public class ShadedTypeParser extends TypeParser {
    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(ShadedTypeParser.class);

    public static final String SHADED_PREFIX = "com.netflix.astyanax.shaded.";

    protected static TypeParser EMPTY_PARSER;

    static {
        try {
            EMPTY_PARSER = new ShadedTypeParser("");
        } catch (ConfigurationException e) {
            Logger.error("ShadedTypeParser failed to create EMPTY_PARSER; message=" + e.getMessage(), e);
        }
    }

    /*******************************************************************************************************************
     * Begin ugly reflection required to work around TypeParser's lack of design for extension:
     ******************************************************************************************************************/
    protected static Field strField;
    protected static Field idxField;
    protected static Field cacheField;
    protected static Map<String, AbstractType<?>> cache;
    protected static Method skipBlankMethod;
    protected static Method skipBlankMethod2;
    protected static Method isEOSMethod;
    protected static Method isEOSMethod2;
    protected static Method isIdentifierCharMethod;

    protected static void init() throws ConfigurationException {
        try {
            strField = ShadedTypeParser.class.getSuperclass().getDeclaredField("str");
            strField.setAccessible(true);

            idxField = ShadedTypeParser.class.getSuperclass().getDeclaredField("idx");
            idxField.setAccessible(true);

            cacheField = ShadedTypeParser.class.getSuperclass().getDeclaredField("cache");
            cacheField.setAccessible(true);
            cache = (Map<String, AbstractType<?>>)cacheField.get(null);

            skipBlankMethod = ShadedTypeParser.class.getSuperclass().getDeclaredMethod("skipBlank");
            skipBlankMethod.setAccessible(true);

            skipBlankMethod2 = ShadedTypeParser.class.getSuperclass().getDeclaredMethod("skipBlank", String.class, int.class);
            skipBlankMethod2.setAccessible(true);

            isEOSMethod = ShadedTypeParser.class.getSuperclass().getDeclaredMethod("isEOS");
            isEOSMethod.setAccessible(true);

            isEOSMethod2 = ShadedTypeParser.class.getSuperclass().getDeclaredMethod("isEOS", String.class, int.class);
            isEOSMethod2.setAccessible(true);

            isIdentifierCharMethod = ShadedTypeParser.class.getSuperclass().getDeclaredMethod("isIdentifierChar", int.class);
            isIdentifierCharMethod.setAccessible(true);

        } catch (NoSuchFieldException | IllegalAccessException | NoSuchMethodException e) {
            throw new ConfigurationException(
                    "ShadedTypeParser init() failed for reflection; message=" + e.getMessage(), e);
        }
    }

    protected String getStr() throws IllegalAccessException {
        return (String)strField.get(this);
    }

    protected int getIdx() throws IllegalAccessException {
        return idxField.getInt(this);
    }

    protected void setIdx(int idx) throws IllegalAccessException {
        idxField.setInt(this, idx);
    }

    protected void skipBlank() throws InvocationTargetException, IllegalAccessException {
        skipBlankMethod.invoke(this);
    }

    protected static int skipBlank(String str, int i) throws InvocationTargetException, IllegalAccessException {
        return (Integer)skipBlankMethod2.invoke(null, str, i);
    }

    protected boolean isEOS() throws InvocationTargetException, IllegalAccessException {
        return (Boolean)isEOSMethod.invoke(this);
    }

    protected static boolean isEOS(String str, int i) throws InvocationTargetException, IllegalAccessException {
        return (Boolean)isEOSMethod2.invoke(null, str, i);
    }

    private static boolean isIdentifierChar(int c) throws InvocationTargetException, IllegalAccessException {
        return (Boolean)isIdentifierCharMethod.invoke(null, c);
    }

    protected static Method getRawAbstractTypeMethod(Class typeClass) throws NoSuchMethodException {
        return ShadedTypeParser.class.getSuperclass().getDeclaredMethod("getRawAbstractType",
                typeClass, EMPTY_PARSER.getClass());
    }

    /*******************************************************************************************************************
     * End ugly reflection required to work around TypeParser's lack of design for extension
     ******************************************************************************************************************/

    public static ShadedTypeParser buildTypeParser(String str, int idx) throws ConfigurationException {
        return new ShadedTypeParser(str, idx);
    }

    public ShadedTypeParser(String str) throws ConfigurationException {
        this(str, 0);
    }

    public ShadedTypeParser(String str, int idx) throws ConfigurationException {
        super(str);
        init();
        try {
            setIdx(idx);
        } catch (IllegalAccessException e) {
            throw new ConfigurationException(
                    "ShadedTypeParser constructor failed for reflection; message=" + e.getMessage(), e);
        }
    }

    public static String getShadedClassName(String className){
        if(className.startsWith(SHADED_PREFIX)){
            return className;
        } else if (className.contains(".")){
            return SHADED_PREFIX + className;
        } else {
            return SHADED_PREFIX + "org.apache.cassandra.db.marshal." + className;
        }
    }

    public static String getShadedTypeName(String typeName){
        if ( typeName.startsWith( "org.apache.cassandra.db.marshal." ) ) {
            return typeName.substring( "org.apache.cassandra.db.marshal.".length() );
        } else if ( typeName.startsWith( SHADED_PREFIX + "org.apache.cassandra.db.marshal." ) ) {
            return typeName.substring( (SHADED_PREFIX + "org.apache.cassandra.db.marshal.").length() );
        }
        return typeName;
    }

    /*******************************************************************************************************************
     * Begin methods very slightly modified from {@link TypeParser} to support shaded classes
     ******************************************************************************************************************/
    public static AbstractType<?> parse(String str) throws SyntaxException, ConfigurationException {
        try{
            if (str == null) {
                return BytesType.instance;
            } else {
                str = getShadedClassName(str);
                AbstractType<?> type = null;
                type = (AbstractType) cache.get(str);
                if (type != null) {
                    return type;
                } else {
                    int i = 0;
                    i = skipBlank(str, i);

                    int j;
                    for (j = i; !isEOS(str, i) && isIdentifierChar(str.charAt(i)); ++i) {
                        ;
                    }

                    if (i == j) {
                        return BytesType.instance;
                    } else {
                        String name = str.substring(j, i);
                        name = getShadedClassName(name);
                        i = skipBlank(str, i);
                        if (!isEOS(str, i) && str.charAt(i) == '(') {
                            ShadedTypeParser typeParser = buildTypeParser(str, i);
                            type = getAbstractType(name, typeParser);
                        } else {
                            type = getAbstractType(name);
                        }

                        cache.put(str, type);
                        return type;
                    }
                }
            }
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new ConfigurationException(
                    "ShadedTypeParser parse(String) failed for reflection; message=" + e.getMessage(), e);
        }
    }

    public AbstractType<?> parse() throws SyntaxException, ConfigurationException {
        try {
            skipBlank();
            String name = this.readNextIdentifier();
            name = getShadedClassName(name);
            skipBlank();
            return !isEOS() && this.getStr().charAt(getIdx()) == '(' ? getAbstractType(name, this) : getAbstractType(name);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new ConfigurationException(
                    "ShadedTypeParser parse() failed for reflection; message=" + e.getMessage(), e);
        }
    }

    protected static AbstractType<?> getAbstractType(String compareWith) throws ConfigurationException {
        String className = getShadedClassName(compareWith);
        Class typeClass = FBUtilities.classForName(className, "abstract-type");

        try {
            Field field = typeClass.getDeclaredField("instance");
            return (AbstractType)field.get((Object)null);
        } catch (NoSuchFieldException | IllegalAccessException var4) {
            try {
                Method getRawAbstractTypeMethod = getRawAbstractTypeMethod(typeClass);
                return (AbstractType<?>) getRawAbstractTypeMethod.invoke(null, typeClass, EMPTY_PARSER);
            } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                throw new ConfigurationException(
                        "ShadedTypeParser getAbstractType failed for reflection; message=" + e.getMessage(), e);
            }
        }
    }

    protected static AbstractType<?> getAbstractType(String compareWith, TypeParser parser) throws SyntaxException, ConfigurationException {
        String className = getShadedClassName(compareWith);
        Class typeClass = FBUtilities.classForName(className, "abstract-type");

        AbstractType type;
        try {
            Method method = typeClass.getDeclaredMethod("getInstance", TypeParser.class);
            return (AbstractType)method.invoke((Object)null, parser);
        } catch (NoSuchMethodException | IllegalAccessException var6) {
            try {
                Method getRawAbstractTypeMethod = getRawAbstractTypeMethod(typeClass);
                type = (AbstractType<?>) getRawAbstractTypeMethod.invoke(null, typeClass);
            } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                throw new ConfigurationException(
                        "ShadedTypeParser getAbstractType() failed for reflection; message=" + e.getMessage(), e);
            }
            return AbstractType.parseDefaultParameters(type, parser);
        } catch (InvocationTargetException var8) {
            ConfigurationException ex = new ConfigurationException("Invalid definition for comparator " + typeClass.getName() + ".");
            ex.initCause(var8.getTargetException());
            throw ex;
        }
    }

    /*******************************************************************************************************************
     * End methods copy-pasted from {@link TypeParser} and modified slightly to to work with shaded classes
     ******************************************************************************************************************/

}
