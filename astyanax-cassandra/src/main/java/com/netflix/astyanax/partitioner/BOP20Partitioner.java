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
package com.netflix.astyanax.partitioner;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Hex;

import com.google.common.collect.Lists;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.impl.TokenRangeImpl;

public class BOP20Partitioner implements Partitioner {
    
    public static final String MINIMUM = "0000000000000000000000000000000000000000";
    public static final String MAXIMUM = "ffffffffffffffffffffffffffffffffffffffff";
    public static final BigInteger ONE = new BigInteger("1", 16);
    public static final int KEY_LENGTH = 20;
    
    @Override
    public String getMinToken() {
        return MINIMUM;
    }

    @Override
    public String getMaxToken() {
        return MAXIMUM;
    }

    @Override
    public List<TokenRange> splitTokenRange(String first, String last, int count) {
        List<TokenRange> tokens = Lists.newArrayList();
        for (int i = 0; i < count; i++) {
            String startToken = getTokenMinusOne(getSegmentToken(count, i, new BigInteger(first, 16), new BigInteger(last, 16)));
            String endToken;
            if (i == count-1 && last.equals(getMaxToken())) 
                endToken = getMinToken();
            else
                endToken = getSegmentToken(count, i+1, new BigInteger(first, 16), new BigInteger(last, 16));
            tokens.add(new TokenRangeImpl(startToken, endToken, new ArrayList<String>()));
        }
        return tokens;
    }

    @Override
    public List<TokenRange> splitTokenRange(int count) {
        return splitTokenRange(getMinToken(), getMaxToken(), count);
    }
    
    @Override
    public String getTokenForKey(ByteBuffer key) {
        if (key.remaining() != KEY_LENGTH) {
            throw new IllegalArgumentException("Key must be a 20 byte array");
        }
        return new String(Hex.encodeHexString(key.duplicate().array()));
    }

    @Override
    public String getTokenMinusOne(String token) {
        if (token.equals("0") || token.equals(MINIMUM))
            return MAXIMUM;
        
        return new BigInteger(token, 16).subtract(ONE).toString(16);
    }

    public static String getSegmentToken(int size, int position, BigInteger minInitialToken, BigInteger maxInitialToken ) {
        BigInteger decValue = minInitialToken;
        if (position != 0)
            decValue = maxInitialToken.multiply(new BigInteger("" + position)).divide(new BigInteger("" + size));
        return decValue.toString(16);
    }
}
