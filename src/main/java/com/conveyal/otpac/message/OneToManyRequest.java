package com.conveyal.otpac.message;

import java.util.Date;

import org.opentripplanner.analyst.PointFeature;
import org.opentripplanner.common.model.GenericLocation;
import org.opentripplanner.routing.core.RoutingRequest;

public class OneToManyRequest {

	public PointFeature from;
	public RoutingRequest options;

	public OneToManyRequest(PointFeature from, RoutingRequest options) {
		this.from = from;
		this.options = options.clone();
		this.options.batch = true;
		this.options.rctx = null;
		this.options.from = new GenericLocation(from.getLat(), from.getLon());
	}

}
