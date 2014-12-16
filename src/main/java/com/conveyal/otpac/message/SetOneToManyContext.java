package com.conveyal.otpac.message;

import java.io.Serializable;

import org.opentripplanner.analyst.SampleSet;
import org.opentripplanner.routing.graph.Graph;

public class SetOneToManyContext implements Serializable {

	public Graph graph;
	public SampleSet to;

	public SetOneToManyContext(Graph graph, SampleSet sampleSet) {
		this.graph = graph;
		this.to = sampleSet;
	}

}
