/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.astyanax.thrift;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.netflix.astyanax.AstyanaxConfiguration;

public class ThriftCqlFactoryResolver {
    private static final Pattern VERSION_REGEX = Pattern.compile("^([0-9])+\\.([0-9])+(.*)");
    
    public static ThriftCqlFactory createFactory(AstyanaxConfiguration config) {
        if (config.getTargetCassandraVersion() != null) {
            Matcher m = VERSION_REGEX.matcher(config.getTargetCassandraVersion());
            if (m.matches()) {
                int major = Integer.parseInt(m.group(1));
                int minor = Integer.parseInt(m.group(2));
                if (major > 1 || (major == 1 && minor >= 2)) {
                    return new ThriftCql3Factory();
                }
            }
        }
        return new ThriftCql2Factory();
    }    
}
