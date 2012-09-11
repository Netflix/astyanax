package com.netflix.astyanax.query;

import java.util.SortedMap;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * Interface for tracking checkpoints for a getAllRows query.  
 * The entire token range is split into a sorted set of start tokens.  Each start token is
 * mapped to a checkpoint with the following possible values
 * 1.  startToken - start of the token range
 * 2.  nextToken  - the checkpoint equals the next token in the sorted set of start tokens.  This means the range is done
 * 3.  > startToken AND < nextToken - a valid checkpoint 
 * 
 * @author elandau
 *
 */
public interface CheckpointManager {
	/**
	 * Trac the checkpoint for a specific range
	 * 
	 * @param start
	 * @param end
	 * @throws Exception 
	 */
	void trackCheckpoint(String startToken, String checkpointToken) throws Exception;
	
	/**
	 * Get the next checkpoint after the specified token 
	 * 
	 * @param start
	 * @param end
	 * @return 
	 */
	String getCheckpoint(String startToken) throws Exception ;
	
	/**
	 * Return a sorted map of start tokens to their checkpoint
	 * @return
	 * @throws ConnectionException 
	 */
	SortedMap<String, String> getCheckpoints() throws Exception;
	
}
