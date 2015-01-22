package com.conveyal.otpac.message;

/**
 * Ask to get a graph.
 */
public class GetGraph {
	public String graphId;

	public GetGraph(String graphId) {
		super();
		this.graphId = graphId;
	}
}
