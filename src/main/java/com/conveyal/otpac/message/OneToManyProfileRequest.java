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
	public ProfileRequest options;

	public OneToManyProfileRequest(PointFeature from, String to, ProfileRequest options, String graphId, int jobId) {
		super(from, to, graphId, jobId, true);
		
		try {
			this.options = options.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		this.options.analyst = true;
		
		// Even though we're making an analyst request, OTP requires a to location
		// which will be ignored
		this.options.fromLat = this.options.toLat = from.getLat();
		this.options.fromLon = this.options.toLon = from.getLon();
	}

	/** used in single point mode with origin specified by options */
	public OneToManyProfileRequest(String to, ProfileRequest options, String graphId, int jobId) {
		super(null, to, graphId, jobId, true);
		try {
			this.options = options.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		this.options.analyst = true;

		this.options.toLat = this.options.fromLat;
		this.options.toLon = this.options.fromLon;
	}
	
	/** used for deserialization from JSON */
	public OneToManyProfileRequest() { /* empty */ }
}
