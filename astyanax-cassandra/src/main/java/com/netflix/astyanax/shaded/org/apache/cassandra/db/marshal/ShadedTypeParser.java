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
import com.netflix.astyanax.shaded.org.apache.cassandra.utils.ByteBufferUtil;
import com.netflix.astyanax.shaded.org.apache.cassandra.utils.FBUtilities;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Extend TypeParser to support shaded {@link AbstractType} from shaded package.
 * Converts input class names to shaded package name.
 *
 * Note that shading prefixes this class's package name with the shaded name {@link #SHADED_PREFIX}.
 *
 * Effectively uses {@link TypeParser} only as an interface. The implementation is a slightly altered copy-paste
 * of {@link TypeParser} because {@link TypeParser#parse(String)} can't really be overridden. That method is public but
 * references private fields (e.g. {@link TypeParser#idx}, etc.) and private methods
 * ({@link TypeParser#getAbstractType}, etc.) which a derived class can't access.
 *
 * This is a very ugly way to support shading cassandra-all, but it's still the least invasive approach.
 */
public class ShadedTypeParser extends TypeParser {
    public static final String SHADED_PREFIX = "com.netflix.astyanax.shaded.";
    protected final String str;
    protected int idx;
    protected static final Map<String, AbstractType<?>> cache = new HashMap();
    protected static final TypeParser EMPTY_PARSER = new ShadedTypeParser("");

    public static ShadedTypeParser buildTypeParser(String str, int idx){
        return new ShadedTypeParser(str, idx);
    }

    public ShadedTypeParser(String str) {
        // TypeParser's private fields aren't actually used, but we're still required to invoke the super constructor
        super(str);
        this.str = str;
        this.idx = 0;
    }

    public ShadedTypeParser(String str, int idx) {
        // TypeParser's private fields aren't actually used, but we're still required to invoke the super constructor
        super(str);
        this.str = str;
        this.idx = idx;
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

    public static AbstractType<?> parse(String str) throws SyntaxException, ConfigurationException {
        if (str == null) {
            return BytesType.instance;
        } else {
            str = getShadedClassName(str);
            AbstractType<?> type = (AbstractType)cache.get(str);
            if (type != null) {
                return type;
            } else {
                int i = 0;
                i = skipBlank(str, i);

                int j;
                for(j = i; !isEOS(str, i) && isIdentifierChar(str.charAt(i)); ++i) {
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
    }

    public AbstractType<?> parse() throws SyntaxException, ConfigurationException {
        this.skipBlank();
        String name = this.readNextIdentifier();
        name = getShadedClassName(name);
        this.skipBlank();
        return !this.isEOS() && this.str.charAt(this.idx) == '(' ? getAbstractType(name, this) : getAbstractType(name);
    }

    public Map<String, String> getKeyValueParameters() throws SyntaxException {
        Map<String, String> map = new HashMap();
        if (this.isEOS()) {
            return map;
        } else if (this.str.charAt(this.idx) != '(') {
            throw new IllegalStateException();
        } else {
            ++this.idx;

            String k;
            String v;
            for(; this.skipBlankAndComma(); map.put(k, v)) {
                if (this.str.charAt(this.idx) == ')') {
                    ++this.idx;
                    return map;
                }

                k = this.readNextIdentifier();
                v = "";
                this.skipBlank();
                if (this.str.charAt(this.idx) == '=') {
                    ++this.idx;
                    this.skipBlank();
                    v = this.readNextIdentifier();
                } else if (this.str.charAt(this.idx) != ',' && this.str.charAt(this.idx) != ')') {
                    this.throwSyntaxError("unexpected character '" + this.str.charAt(this.idx) + "'");
                }
            }

            throw new SyntaxException(String.format("Syntax error parsing '%s' at char %d: unexpected end of string", this.str, this.idx));
        }
    }

    public List<AbstractType<?>> getTypeParameters() throws SyntaxException, ConfigurationException {
        List<AbstractType<?>> list = new ArrayList();
        if (this.isEOS()) {
            return list;
        } else if (this.str.charAt(this.idx) != '(') {
            throw new IllegalStateException();
        } else {
            ++this.idx;

            while(this.skipBlankAndComma()) {
                if (this.str.charAt(this.idx) == ')') {
                    ++this.idx;
                    return list;
                }

                try {
                    list.add(this.parse());
                } catch (SyntaxException var4) {
                    SyntaxException ex = new SyntaxException(String.format("Exception while parsing '%s' around char %d", this.str, this.idx));
                    ex.initCause(var4);
                    throw ex;
                }
            }

            throw new SyntaxException(String.format("Syntax error parsing '%s' at char %d: unexpected end of string", this.str, this.idx));
        }
    }

    public Map<Byte, AbstractType<?>> getAliasParameters() throws SyntaxException, ConfigurationException {
        Map<Byte, AbstractType<?>> map = new HashMap();
        if (this.isEOS()) {
            return map;
        } else if (this.str.charAt(this.idx) != '(') {
            throw new IllegalStateException();
        } else {
            ++this.idx;

            while(this.skipBlankAndComma()) {
                if (this.str.charAt(this.idx) == ')') {
                    ++this.idx;
                    return map;
                }

                String alias = this.readNextIdentifier();
                if (alias.length() != 1) {
                    this.throwSyntaxError("An alias should be a single character");
                }

                char aliasChar = alias.charAt(0);
                if (aliasChar < '!' || aliasChar > 127) {
                    this.throwSyntaxError("An alias should be a single character in [0..9a..bA..B-+._&]");
                }

                this.skipBlank();
                if (this.str.charAt(this.idx) != '=' || this.str.charAt(this.idx + 1) != '>') {
                    this.throwSyntaxError("expecting '=>' token");
                }

                this.idx += 2;
                this.skipBlank();

                try {
                    map.put((byte)aliasChar, this.parse());
                } catch (SyntaxException var6) {
                    SyntaxException ex = new SyntaxException(String.format("Exception while parsing '%s' around char %d", this.str, this.idx));
                    ex.initCause(var6);
                    throw ex;
                }
            }

            throw new SyntaxException(String.format("Syntax error parsing '%s' at char %d: unexpected end of string", this.str, this.idx));
        }
    }

    public Map<ByteBuffer, CollectionType> getCollectionsParameters() throws SyntaxException, ConfigurationException {
        Map<ByteBuffer, CollectionType> map = new HashMap();
        if (this.isEOS()) {
            return map;
        } else if (this.str.charAt(this.idx) != '(') {
            throw new IllegalStateException();
        } else {
            ++this.idx;

            while(this.skipBlankAndComma()) {
                if (this.str.charAt(this.idx) == ')') {
                    ++this.idx;
                    return map;
                }

                String bbHex = this.readNextIdentifier();
                ByteBuffer bb = null;

                try {
                    bb = ByteBufferUtil.hexToBytes(bbHex);
                } catch (NumberFormatException var7) {
                    this.throwSyntaxError(var7.getMessage());
                }

                this.skipBlank();
                if (this.str.charAt(this.idx) != ':') {
                    this.throwSyntaxError("expecting ':' token");
                }

                ++this.idx;
                this.skipBlank();

                try {
                    AbstractType<?> type = this.parse();
                    if (!(type instanceof CollectionType)) {
                        throw new SyntaxException(type.toString() + " is not a collection type");
                    }

                    map.put(bb, (CollectionType)type);
                } catch (SyntaxException var6) {
                    SyntaxException ex = new SyntaxException(String.format("Exception while parsing '%s' around char %d", this.str, this.idx));
                    ex.initCause(var6);
                    throw ex;
                }
            }

            throw new SyntaxException(String.format("Syntax error parsing '%s' at char %d: unexpected end of string", this.str, this.idx));
        }
    }

    protected static AbstractType<?> getAbstractType(String compareWith) throws ConfigurationException {
        String className = getShadedClassName(compareWith);
        Class typeClass = FBUtilities.classForName(className, "abstract-type");

        try {
            Field field = typeClass.getDeclaredField("instance");
            return (AbstractType)field.get((Object)null);
        } catch (NoSuchFieldException var4) {
            return getRawAbstractType(typeClass, EMPTY_PARSER);
        } catch (IllegalAccessException var5) {
            return getRawAbstractType(typeClass, EMPTY_PARSER);
        }
    }

    protected static AbstractType<?> getAbstractType(String compareWith, TypeParser parser) throws SyntaxException, ConfigurationException {
        String className = getShadedClassName(compareWith);
        Class typeClass = FBUtilities.classForName(className, "abstract-type");

        AbstractType type;
        try {
            Method method = typeClass.getDeclaredMethod("getInstance", TypeParser.class);
            return (AbstractType)method.invoke((Object)null, parser);
        } catch (NoSuchMethodException var6) {
            type = getRawAbstractType(typeClass);
            return AbstractType.parseDefaultParameters(type, parser);
        } catch (IllegalAccessException var7) {
            type = getRawAbstractType(typeClass);
            return AbstractType.parseDefaultParameters(type, parser);
        } catch (InvocationTargetException var8) {
            ConfigurationException ex = new ConfigurationException("Invalid definition for comparator " + typeClass.getName() + ".");
            ex.initCause(var8.getTargetException());
            throw ex;
        }
    }

    protected static AbstractType<?> getRawAbstractType(Class<? extends AbstractType<?>> typeClass) throws ConfigurationException {
        try {
            Field field = typeClass.getDeclaredField("instance");
            return (AbstractType)field.get((Object)null);
        } catch (NoSuchFieldException var2) {
            throw new ConfigurationException("Invalid comparator class " + typeClass.getName() + ": must define a public static instance field or a public static method getInstance(TypeParser).");
        } catch (IllegalAccessException var3) {
            throw new ConfigurationException("Invalid comparator class " + typeClass.getName() + ": must define a public static instance field or a public static method getInstance(TypeParser).");
        }
    }

    protected static AbstractType<?> getRawAbstractType(Class<? extends AbstractType<?>> typeClass, TypeParser parser) throws ConfigurationException {
        try {
            Method method = typeClass.getDeclaredMethod("getInstance", TypeParser.class);
            return (AbstractType)method.invoke((Object)null, parser);
        } catch (NoSuchMethodException var4) {
            throw new ConfigurationException("Invalid comparator class " + typeClass.getName() + ": must define a public static instance field or a public static method getInstance(TypeParser).");
        } catch (IllegalAccessException var5) {
            throw new ConfigurationException("Invalid comparator class " + typeClass.getName() + ": must define a public static instance field or a public static method getInstance(TypeParser).");
        } catch (InvocationTargetException var6) {
            ConfigurationException ex = new ConfigurationException("Invalid definition for comparator " + typeClass.getName() + ".");
            ex.initCause(var6.getTargetException());
            throw ex;
        }
    }

    protected void throwSyntaxError(String msg) throws SyntaxException {
        throw new SyntaxException(String.format("Syntax error parsing '%s' at char %d: %s", this.str, this.idx, msg));
    }

    protected boolean isEOS() {
        return isEOS(this.str, this.idx);
    }

    protected static boolean isEOS(String str, int i) {
        return i >= str.length();
    }

    protected static boolean isBlank(int c) {
        return c == 32 || c == 9 || c == 10;
    }

    protected void skipBlank() {
        this.idx = skipBlank(this.str, this.idx);
    }

    protected static int skipBlank(String str, int i) {
        while(!isEOS(str, i) && isBlank(str.charAt(i))) {
            ++i;
        }

        return i;
    }

    protected boolean skipBlankAndComma() {
        for(boolean commaFound = false; !this.isEOS(); ++this.idx) {
            int c = this.str.charAt(this.idx);
            if (c == ',') {
                if (commaFound) {
                    return true;
                }

                commaFound = true;
            } else if (!isBlank(c)) {
                return true;
            }
        }

        return false;
    }

    protected static boolean isIdentifierChar(int c) {
        return c >= 48 && c <= 57 || c >= 97 && c <= 122 || c >= 65 && c <= 90 || c == 45 || c == 43 || c == 46 || c == 95 || c == 38;
    }

    public String readNextIdentifier() {
        int i;
        for(i = this.idx; !this.isEOS() && isIdentifierChar(this.str.charAt(this.idx)); ++this.idx) {
            ;
        }

        return this.str.substring(i, this.idx);
    }

    public char readNextChar() {
        this.skipBlank();
        return this.str.charAt(this.idx++);
    }
}
