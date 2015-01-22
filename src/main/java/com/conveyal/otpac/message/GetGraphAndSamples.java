package com.conveyal.otpac.message;

import java.io.Serializable;

/**
 * Get the graph and the sampleset for the given graph ID and pointset ID.
 */
public class GetGraphAndSamples implements Serializable {
	public String graphId;
	public String pointsetId;

	public GetGraphAndSamples(String graphId, String pointsetId) {
		this.graphId = graphId;
		this.pointsetId = pointsetId;
	}
}
