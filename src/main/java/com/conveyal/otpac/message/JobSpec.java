package com.conveyal.otpac.message;

import java.io.Serializable;
import java.util.List;

import org.opentripplanner.profile.ProfileRequest;
import org.opentripplanner.routing.core.RoutingRequest;

import com.conveyal.otpac.JobItemCallback;

public class JobSpec implements Serializable{

	private static final long serialVersionUID = 6683822607915423812L;
	
	public String graphId;
	public int jobId;
	public String fromPtsLoc;
	public String toPtsLoc;

	/** Vanilla routing: routing parameters */
	public RoutingRequest options;
	
	/** Profile routing: routing parameters */
	public ProfileRequest profileOptions;
	
	/** Are we using profile routing? */
	public boolean profileRouting;
	
	public List<String> subsetIds = null;

	public JobItemCallback callback=null;

	/**
	 * Create a job using vanilla routing for the specified RoutingRequest.
	 */
	public JobSpec(String graphId, String fromPtsLoc, String toPtsLoc, RoutingRequest options) {
		this(graphId, fromPtsLoc, toPtsLoc, null, options);
	}
	
	/**
	 * Create a job using vanilla routing for the specified RoutingRequest.
	 */
	public JobSpec(String graphId, String fromPtsLoc, String toPtsLoc, List<String> subsetIds, RoutingRequest options) {
		this.graphId = graphId;
		this.fromPtsLoc = fromPtsLoc;
		this.toPtsLoc = toPtsLoc;
		
		this.options = options;
		this.profileOptions = null;
		this.profileRouting = false;
		
		this.subsetIds = subsetIds;
	}
	
	/**
	 * Create a job using profile routing for the specified ProfileRequest.
	 */
	public JobSpec(String graphId, String fromPtsLoc, String toPtsLoc, ProfileRequest options) {
		this(graphId, fromPtsLoc, toPtsLoc, null, options);
	}
	
	/**
	 * Create a job using profile routing for the specified ProfileRequest.
	 */
	public JobSpec(String graphId, String fromPtsLoc, String toPtsLoc, List<String> subsetIds, ProfileRequest options) {
		this.graphId = graphId;
		this.fromPtsLoc = fromPtsLoc;
		this.toPtsLoc = toPtsLoc;
		
		this.options = null;
		this.profileOptions = options;
		this.profileRouting = true;
		
		this.subsetIds = subsetIds;
	}

	public void setCallback(JobItemCallback callback) {
		this.callback = callback;
	}

}
