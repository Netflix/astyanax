package com.netflix.astyanax.cql.reads;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;

public abstract class QueryGenCache<Q> {

	private final Session session; 
	private final AtomicReference<PreparedStatement> cachedStatement = new AtomicReference<PreparedStatement>(null);

	public QueryGenCache(Session session) {
		this.session = session;
	}

	public abstract Callable<RegularStatement> getQueryGen(Q query);

	public BoundStatement getBoundStatement(Q query, boolean useCaching) {

		PreparedStatement pStatement = getPreparedStatement(query, useCaching);
		return bindValues(pStatement, query);
	}

	public abstract BoundStatement bindValues(PreparedStatement pStatement, Q query);

	public PreparedStatement getPreparedStatement(Q query, boolean useCaching) {

		PreparedStatement pStatement = null;

		if (useCaching) {
			pStatement = cachedStatement.get();
		}

		if (pStatement == null) {
			try {
				RegularStatement stmt = getQueryGen(query).call();
				//System.out.println("Query: " + query.getQueryString());
				System.out.println("query " + stmt.getQueryString());
				pStatement = session.prepare(stmt.getQueryString());
				System.out.println("pStatement " + pStatement.getQueryString());
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		if (useCaching && cachedStatement.get() == null) {
			cachedStatement.set(pStatement);
		}
		return pStatement;
	}
}
