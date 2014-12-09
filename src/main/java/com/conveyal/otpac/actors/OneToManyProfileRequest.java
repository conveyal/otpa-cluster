package com.conveyal.otpac.actors;

import org.opentripplanner.analyst.PointFeature;
import org.opentripplanner.api.param.LatLon;
import org.opentripplanner.profile.ProfileRequest;

/**
 * Represents a request for a profile search from a given point to all vertices in the graph.
 * @author mattwigway
 *
 */
public class OneToManyProfileRequest {
	public PointFeature from;
	public ProfileRequest options;

	public OneToManyProfileRequest(PointFeature from, ProfileRequest options) {
		this.from = from;
		this.options = options.clone();
		this.options.analyst = true;
		this.options.from = new LatLon(from.getLat(), from.getLon());
	}
}
