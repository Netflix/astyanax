package com.netflix.astyanax.connectionpool.exceptions;


public class BadConfigurationException extends ConnectionException {
	private String parameter;
	private String value;
	private String expected;
	
    public BadConfigurationException(String parameter, String value, String expected) {
        super(parameter + ":" + value + ":" + expected, false);
    }

    public BadConfigurationException(Throwable cause) {
        super(cause, false);
    }

    public BadConfigurationException(String parameter, String value, String expected, Throwable cause) {
        super(parameter + ":" + value + ":" + expected, cause, false);
    }

	public String getParameter() { return parameter; }
	public String getValue() { return value; }
	public String getExpected() { return expected; }
}
