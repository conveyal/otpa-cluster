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
		// putting the lat lons into a string is ugly, but there's not really a better
		// way to do it without major changes in OTP.
		this.options.from = new LatLon(String.format("%f,%f", from.getLat(), from.getLon()));
		
		// Even though we're making an analyst request, OTP requires a to location
		// which will be ignored
		this.options.to = this.options.from;
	}
}
