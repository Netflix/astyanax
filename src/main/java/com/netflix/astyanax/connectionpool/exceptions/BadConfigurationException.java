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
package com.netflix.astyanax.connectionpool.exceptions;

public class BadConfigurationException extends OperationException {
    /**
     * 
     */
    private static final long serialVersionUID = -5029835858453307841L;
    private String parameter;
    private String value;
    private String expected;

    public BadConfigurationException(String parameter, String value, String expected) {
        super("Bad configuration " + parameter + "=" + value + " (expected: " + expected + ")");
    }

    public BadConfigurationException(Throwable cause) {
        super(cause);
    }

    public BadConfigurationException(String parameter, String value, String expected, Throwable cause) {
        super("Bad configuration " + parameter + "=" + value + " (expected: " + expected + ")", cause);
    }

    public BadConfigurationException(String parameter, String value, Throwable cause) {
        super("Bad configuration " + parameter + "=" + value, cause);
    }

    public String getParameter() {
        return parameter;
    }

    public String getValue() {
        return value;
    }

    public String getExpected() {
        return expected;
    }

    @Override
    public String getMessage() {
        return this.getOriginalMessage();
    }
}
