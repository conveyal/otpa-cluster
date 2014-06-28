package com.conveyal.akkaplay.message;

import org.opentripplanner.analyst.PointSet;
import org.opentripplanner.routing.graph.Graph;

public class SetOneToManyContext {

	public Graph graph;
	public PointSet to;

	public SetOneToManyContext(Graph graph, PointSet to) {
		this.graph = graph;
		this.to = to;
	}

}
