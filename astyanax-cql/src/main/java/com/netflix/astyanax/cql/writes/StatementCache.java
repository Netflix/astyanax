package com.netflix.astyanax.cql.writes;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import com.datastax.driver.core.PreparedStatement;

public class StatementCache {

	private final ConcurrentHashMap<Integer, PreparedStatement> statementCache = new ConcurrentHashMap<Integer, PreparedStatement>();
	
	private StatementCache() {
		
	}
	
	public PreparedStatement getStatement(Integer id) {
		return statementCache.get(id);
	}
	
	public PreparedStatement getStatement(Integer id, Callable<PreparedStatement> func) {
		
		PreparedStatement stmt = statementCache.get(id);
		if (stmt == null) {
			try {
				stmt = func.call();
				statementCache.putIfAbsent(id, stmt);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return stmt;
	}
	
	private static final StatementCache Instance = new StatementCache();
	
	public static StatementCache getInstance() {
		return Instance;
	}
}
