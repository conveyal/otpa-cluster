package com.conveyal.otpac.message;

import java.io.Serializable;
import java.util.Date;

import org.apache.commons.math3.stat.clustering.Clusterable;
import org.opentripplanner.analyst.PointFeature;
import org.opentripplanner.common.model.GenericLocation;
import org.opentripplanner.routing.core.RoutingRequest;

public class OneToManyRequest extends AnalystClusterRequest implements Serializable {

	public PointFeature from;
	public RoutingRequest options;

	public OneToManyRequest(PointFeature from, String to, RoutingRequest options, String graphId) {
		super(to, graphId);
		
		this.from = from;
		this.options = options.clone();
		this.options.batch = true;
		this.options.rctx = null;
		this.options.from = new GenericLocation(from.getLat(), from.getLon());
	}
}
