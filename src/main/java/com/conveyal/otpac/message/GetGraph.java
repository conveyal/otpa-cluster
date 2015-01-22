package com.conveyal.otpac.message;

import java.io.Serializable;

/**
 * Ask to get a graph.
 */
public class GetGraph implements Serializable {
	public String graphId;

	public GetGraph(String graphId) {
		super();
		this.graphId = graphId;
	}
}
