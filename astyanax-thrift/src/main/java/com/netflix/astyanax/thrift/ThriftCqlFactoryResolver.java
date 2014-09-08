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
