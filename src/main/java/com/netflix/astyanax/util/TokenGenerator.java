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
package com.netflix.astyanax.util;

import java.math.BigInteger;

public class TokenGenerator {
    public static final BigInteger MINIMUM = new BigInteger("" + 0);
    public static final BigInteger MAXIMUM = new BigInteger("" + 2).pow(127);

    public static String initialToken(int size, int position) {
        return TokenGenerator.initialToken(size,position,MINIMUM,MAXIMUM);
    }

    public static String initialToken(int size, int position, BigInteger minInitialToken, BigInteger maxInitialToken ) {
        BigInteger decValue = minInitialToken;
        if (position != 0)
            decValue = maxInitialToken.divide(new BigInteger("" + size)).multiply(new BigInteger("" + position));
        return decValue.toString();
    }

    public static String tokenMinusOne(String payload) {
        BigInteger bigInt = new BigInteger(payload);
        // if zero rotate to the Maximum else minus one.
        if (bigInt.equals(MINIMUM))
            bigInt = MAXIMUM;
        bigInt = bigInt.subtract(new BigInteger("1"));
        return bigInt.toString();
    }

    public static String getMaximumToken() {
        return MAXIMUM.toString();
    }

    public static String getMinimumToken() {
        return MINIMUM.toString();
    }
}
