package com.conveyal.otpac.message;

import java.io.Serializable;

/**
 * Summarize the status of a node in the cluster.
 * @author mattwigway
 *
 */
public class WorkerStatus implements Serializable {
	/** The size of this worker's queue */
	public int queueSize;
	
	/** The number of requests this worker asks for when its queue runs dry */
	public int chunkSize;
	
	/** The graph this worker currently has in memory */
	public String graph;
	
	/** Is this worker currently building a graph or waiting to do so? */
	public boolean buildingGraph;
	
	public WorkerStatus(int queueSize, int chunkSize, String graph, boolean buildingGraph) {
		this.queueSize = queueSize;
		this.chunkSize = chunkSize;
		this.graph = graph;
		this.buildingGraph = buildingGraph;
	}
	
	public String toString () {
		return "Status " + queueSize + " queued items, chunk size " + chunkSize + " on graph " + graph + (buildingGraph ? ", building" : "");
	}
}
