package com.netflix.astyanax.mock;

import java.util.List;

import com.netflix.astyanax.model.TokenRange;

public class MockTokenRange implements TokenRange {
	private String start;
	private String end;
	private List<String> endpoints;
	
	public MockTokenRange(String start, String end, List<String> endpoints) {
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

}
