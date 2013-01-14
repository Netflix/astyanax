/*******************************************************************************
 * Copyright 2013 Netflix
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
package com.netflix.astyanax.connectionpool;

import java.util.Arrays;
import java.util.List;

public class SSLConnectionContext
{
    public static final String DEFAULT_SSL_PROTOCOL = "TLS";
    public static final List<String> DEFAULT_SSL_CIPHER_SUITES = Arrays.asList("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA");

    private final String sslProtocol;
    private final List<String> sslCipherSuites;
    private final String sslTruststore;
    private final String sslTruststorePassword;

    public SSLConnectionContext(String sslTruststore, String sslTruststorePassword){
        this(sslTruststore, sslTruststorePassword, DEFAULT_SSL_PROTOCOL, DEFAULT_SSL_CIPHER_SUITES);
    }

    public SSLConnectionContext(String sslTruststore, String sslTruststorePassword, String sslProtocol, List<String> sslCipherSuites){
        this.sslTruststore = sslTruststore;
        this.sslTruststorePassword = sslTruststorePassword;
        this.sslProtocol = sslProtocol;
        this.sslCipherSuites = sslCipherSuites;
    }

    /** SSL protocol (typically, TLS) */
    public String getSslProtocol(){
        return sslProtocol;
    }

    /**
     * The SSL ciphers to use. Common examples, often default, are TLS_RSA_WITH_AES_128_CBC_SHA and
     * TLS_RSA_WITH_AES_256_CBC_SHA
     */
    public List<String> getSslCipherSuites() {
        return sslCipherSuites;
    }

    public String getSslTruststore() {
        return sslTruststore;
    }

    public String getSslTruststorePassword() {
        return sslTruststorePassword;
    }
}
