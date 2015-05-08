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
	
	/** Should times be included in the results (i.e. ResultSetWithTimes rather than ResultSet) */
	public boolean includeTimes = false;
	
	/** Is this a profile request? */
	public boolean profile;
	
	public AnalystClusterRequest(PointFeature from, String destinationPointsetId, String graphId, int jobId, boolean profile) {
		this.from = from;
		this.destinationPointsetId = destinationPointsetId;
		this.graphId = graphId;
		this.jobId = jobId;
		this.profile = profile;
	}
	
	/** used for deserialization from JSON */
	public AnalystClusterRequest () { /* do nothing */ }
}
