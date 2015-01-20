package com.conveyal.otpac.message;

import java.io.Serializable;

import org.opentripplanner.analyst.PointFeature;
import org.opentripplanner.api.param.LatLon;
import org.opentripplanner.profile.ProfileRequest;

/**
 * Represents a request for a profile search from a given point to all vertices in the graph.
 * @author mattwigway
 *
 */
public class OneToManyProfileRequest extends AnalystClusterRequest implements Serializable {
	public PointFeature from;
	public ProfileRequest options;

	public OneToManyProfileRequest(PointFeature from, String to, ProfileRequest options, String graphId) {
		super(to, graphId);
		
		this.from = from;
		this.options = options.clone();
		this.options.analyst = true;
		
		// Even though we're making an analyst request, OTP requires a to location
		// which will be ignored
		this.options.fromLat = this.options.toLat = from.getLat();
		this.options.fromLon = this.options.toLon = from.getLon();
	}
}
