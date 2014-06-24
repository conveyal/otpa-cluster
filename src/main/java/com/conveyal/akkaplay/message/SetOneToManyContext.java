package com.conveyal.akkaplay.message;

import org.opentripplanner.routing.graph.Graph;

import com.conveyal.akkaplay.Pointset;

public class SetOneToManyContext {

	public Graph graph;
	public Pointset to;

	public SetOneToManyContext(Graph graph, Pointset to) {
		this.graph = graph;
		this.to = to;
	}

}
