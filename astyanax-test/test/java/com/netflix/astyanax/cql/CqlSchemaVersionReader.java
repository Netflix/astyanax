package com.netflix.astyanax.cql;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * Simple class that reads the schema versions from the system local and peers table. 
 * 
 * @author poberai
 */
public class CqlSchemaVersionReader {

	private static final Logger Logger = LoggerFactory.getLogger(CqlSchemaVersionReader.class);
	
	private static final String SELECT_SCHEMA_LOCAL = "SELECT schema_version FROM system.local WHERE key='local'";
	private static final String SELECT_SCHEMA_PEERS = "SELECT peer, schema_version FROM system.peers";

	private final Session session;
	
	public CqlSchemaVersionReader(Session session) { 
		this.session = session;
	}
	
	public Map<String, List<String>> exec() {
		
	    Map<String, List<String>> versions = new HashMap<String, List<String>>();

		ResultSet rs = session.execute(SELECT_SCHEMA_LOCAL);
		
        Row localRow = rs.one();
        if (localRow != null && !localRow.isNull("schema_version")) {
        	UUID localSchemaVersion = localRow.getUUID("schema_version");
        	InetAddress localServer = rs.getExecutionInfo().getQueriedHost().getAddress();
            addSchemaVersion(localSchemaVersion, localServer, versions);
        }

		rs = session.execute(SELECT_SCHEMA_PEERS);

        for (Row row : rs.all()) {

        	if (row.isNull("rpc_address") || row.isNull("schema_version"))
                continue;
            
        	UUID schema = row.getUUID("schema_version");
            InetAddress remoteEndpoint = row.getInet("rpc_address");
            addSchemaVersion(schema, remoteEndpoint, versions);
        }

        if (Logger.isDebugEnabled()) {
        	Logger.debug("Checking for schema agreement: versions are {}", versions);
        }
        
        return versions;
	}
	
	private void addSchemaVersion(UUID versionUUID, InetAddress endpoint, Map<String, List<String>> map) {
		
		String version = versionUUID.toString();
		List<String> endpoints = map.get(version);
		if (endpoints == null) {
			endpoints = new ArrayList<String>();
			map.put(version, endpoints);
		}
		endpoints.add(endpoint.toString());
	}
}
