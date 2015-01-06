package com.conveyal.otpac.message;

/**
 * Tell a workermanager to process some cluster requests.
 */
public class ProcessClusterRequests {
	public AnalystClusterRequest[] requests;
	
	public ProcessClusterRequests(AnalystClusterRequest... requests) {
		this.requests = requests; 
	}
}
