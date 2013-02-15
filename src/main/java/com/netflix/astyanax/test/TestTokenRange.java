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
package com.netflix.astyanax.test;

import java.math.BigInteger;
import java.util.List;

import com.google.common.collect.Lists;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.impl.TokenRangeImpl;
import com.netflix.astyanax.util.TokenGenerator;

public class TestTokenRange implements TokenRange {
    private String start;
    private String end;
    private List<String> endpoints;

    public TestTokenRange(String start, String end, List<String> endpoints) {
        this.start = start;
        this.end = end;
        this.endpoints = endpoints;
    }

    @Override
    public String getStartToken() {
        return start;
    }

    @Override
    public String getEndToken() {
        return end;
    }

    @Override
    public List<String> getEndpoints() {
        return endpoints;
    }
    
    public static List<Host> makeRing(
            int nHosts,
            int replication_factor, 
            int id, 
            BigInteger minInitialToken, 
            BigInteger maxInitialToken) {
        
        List<Host> hosts = Lists.newArrayList();
        for (int i = 0; i < nHosts; i++) {
            hosts.add(new Host("127.0." + id + "." + i + ":" + TestHostType.GOOD_FAST.ordinal(), 9160));
        }

        for (int i = 0; i < nHosts; i++) {
            String startToken = TokenGenerator.initialToken(nHosts, i,   minInitialToken, maxInitialToken);
            String endToken   = TokenGenerator.initialToken(nHosts, i+1, minInitialToken, maxInitialToken);
            if (endToken.equals(maxInitialToken.toString()))
                endToken = minInitialToken.toString();
            TokenRange range = new TokenRangeImpl(startToken, endToken, null);
            
            for (int j = 0; j < replication_factor; j++) {
                hosts.get((i+j)%nHosts).getTokenRanges().add(range);
            }
        }

        return hosts;
    }
    
    public static String getRingDetails(List<Host> hosts) {
        StringBuilder sb = new StringBuilder();
        for (Host host : hosts) {
            sb.append(host.toString())
              .append("\n");
            
            for (TokenRange range : host.getTokenRanges()) {
                sb.append("  ").append(range.toString()).append("\n");
            }
        }
        
        return sb.toString();
    }

}
