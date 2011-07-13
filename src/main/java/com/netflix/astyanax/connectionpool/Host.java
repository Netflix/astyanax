package com.netflix.astyanax.connectionpool;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Host {

	private final String host;
	private final String ipAddress;
	private final int port;
	private final String name;
	private final String url;
	
	public Host(String url2, int defaultPort) {
		
		String tempHost = parseHostFromUrl(url2);
	    this.port = parsePortFromUrl(url2, defaultPort);
	    
	    String workHost;
	    String workIpAddress;
	    try {
	    	InetAddress address = InetAddress.getByName(tempHost);
	    	workHost 		    = address.getHostName();
	    	workIpAddress 		= address.getHostAddress();
	    } catch (UnknownHostException e) {
	    	workHost			= tempHost;
	    	workIpAddress 		= tempHost;
	    }
	    this.host = workHost;
	    this.ipAddress = workIpAddress;

	    this.name = String.format("%s(%s):%d", tempHost, this.ipAddress, this.port);
	    this.url  = String.format("%s:%d", this.host, this.port);
	}
	
	/**
	 * Parse the hostname from a "hostname:port" formatted string
	 * 
	 * @param urlPort
	 * @return
	 */
	public static String parseHostFromUrl(String urlPort) {
		return urlPort.lastIndexOf(':') > 0 ? urlPort.substring(0, urlPort.lastIndexOf(':')) : urlPort;
	}

	/**
	 * Parse the port from a "hostname:port" formatted string
	 * @param urlPort
	 * @param defaultPort
	 * @return
	 */
	public static int parsePortFromUrl(String urlPort, int defaultPort) {
		return urlPort.lastIndexOf(':') > 0 ? Integer.valueOf(urlPort.substring(urlPort.lastIndexOf(':')+1, urlPort.length())) : defaultPort;
	}

	@Override
	public String toString() {
	    return name;
	}

	public boolean equals(Object obj) {
	    if (! (obj instanceof Host)) {
	    	return false;
	    }
	    Host other = (Host) obj;
	    return other.ipAddress.equals(ipAddress) && other.port == port;
	}

	public int hashCode() {
	    return ipAddress.hashCode();
	}
	  
	public String getName() {
		return this.name;
	}
	
	public String getUrl() {
		return this.url;
	}

	public String getIpAddress() {
		return this.ipAddress;
	}

	public String getHostName() {
		return this.host;
	}

	public int getPort() {
		return this.port;
	}
}
