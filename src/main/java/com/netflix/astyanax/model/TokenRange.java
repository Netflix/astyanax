package com.netflix.astyanax.model;

import java.util.List;

public interface TokenRange {
	String getStartToken();
	String getEndToken();
	List<String> getEndpoints();
}
