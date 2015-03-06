package com.conveyal.otpac.message;

import java.io.Serializable;
import java.util.Date;

import org.opentripplanner.analyst.PointFeature;
import org.opentripplanner.common.model.GenericLocation;
import org.opentripplanner.routing.core.RoutingRequest;

public class OneToManyRequest extends AnalystClusterRequest implements Serializable {
	public RoutingRequest options;

	public OneToManyRequest(PointFeature from, String to, RoutingRequest options, String graphId, int jobId) {
		super(from, to, graphId, jobId);
		
		this.options = options.clone();
		this.options.batch = true;
		this.options.rctx = null;
		this.options.from = new GenericLocation(from.getLat(), from.getLon());
	}

	/** used for single point requests with from specified by options */
	public OneToManyRequest(String to, RoutingRequest options, String graphId, int jobId) {
		super(null, to, graphId, jobId);

		this.options = options.clone();
		this.options.batch = true;
		this.options.rctx = null;
	}
}
