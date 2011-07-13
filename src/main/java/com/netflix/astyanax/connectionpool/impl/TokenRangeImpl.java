package com.netflix.astyanax.connectionpool.impl;

import java.util.List;

import com.google.inject.internal.ImmutableList;
import com.netflix.astyanax.model.TokenRange;

public class TokenRangeImpl implements TokenRange {

	private final String startToken;
	private final String endToken;
	private final List<String> endpoints;
	
	public TokenRangeImpl(String startToken, String endToken, List<String> endpoints) {
		this.startToken = startToken;
		this.endToken = endToken;
		this.endpoints = ImmutableList.copyOf(endpoints);
	}
	
	@Override
	public String getStartToken() {
		return this.startToken;
	}

	@Override
	public String getEndToken() {
		return this.endToken;
	}

	@Override
	public List<String> getEndpoints() {
		return this.endpoints;
	}
	
}
