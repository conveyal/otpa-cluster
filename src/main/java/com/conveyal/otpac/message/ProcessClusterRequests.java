package com.conveyal.otpac.message;

import java.io.Serializable;

/**
 * Tell a workermanager to process some cluster requests.
 */
public class ProcessClusterRequests implements Serializable {
	public AnalystClusterRequest[] requests;
	public String graphId;
	
	public ProcessClusterRequests(String graphId, AnalystClusterRequest... requests) {
		this.requests = requests;
		this.graphId = graphId;
	}
}
