package com.conveyal.otpac.message;

import java.io.Serializable;

import org.opentripplanner.analyst.PointFeature;
import org.opentripplanner.analyst.PointSet;
import org.opentripplanner.analyst.SampleSet;

import com.conveyal.otpac.PointSetDatastore;

/**
 * Marker interface for requests sent to an SPTWorker.
 * @author matthewc
 *
 */
public abstract class AnalystClusterRequest implements Serializable {
	/** The ID of the destinations pointset */
	public String destinationPointsetId;
	
	/** The ID of the graph against which to calculate this request */
	public String graphId;
	
	/** The origin */
	public PointFeature from;
	
	/** The job ID this is associated with */
	public int jobId;
	
	/**
	 * The destinations are passed to the SPTWorker here.
	 * Marked as transient because we never want to serialize the destinations,
	 * but we do want to pass them within local VMs.
	 */
	public transient SampleSet destinations;
	
	public AnalystClusterRequest(PointFeature from, String destinationPointsetId, String graphId, int jobId) {
		this.from = from;
		this.destinationPointsetId = destinationPointsetId;
		this.graphId = graphId;
		this.jobId = jobId;
	}
}
