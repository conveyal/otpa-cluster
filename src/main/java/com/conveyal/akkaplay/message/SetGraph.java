package com.conveyal.akkaplay.message;

import org.opentripplanner.routing.graph.Graph;

public class SetGraph {

	public Graph graph;

	public SetGraph(Graph graph) {
		this.graph = graph;
	}

}
