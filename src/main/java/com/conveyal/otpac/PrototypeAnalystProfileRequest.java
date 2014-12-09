package com.conveyal.otpac;

import org.opentripplanner.profile.Option;
import org.opentripplanner.profile.ProfileRequest;
import org.opentripplanner.routing.core.TraverseModeSet;

/**
 * A profile request with reasonable defaults.
 * 
 * @author mattwigway
 */
public class PrototypeAnalystProfileRequest extends ProfileRequest {
	public PrototypeAnalystProfileRequest () {
		walkSpeed = 1.4f;
		bikeSpeed = 4.1f;
		carSpeed = 20f;
		
		streetTime = 90;
		maxWalkTime = 45;
		maxBikeTime = 45;
		maxCarTime = 45;
		minBikeTime = 10;
		minCarTime = 10;
		
		// doesn't matter for analyst requests
		orderBy = Option.SortOrder.AVG;
		
		analyst = true;
		
		limit = 10;
		suboptimalMinutes = 5;
		accessModes = new TraverseModeSet("WALK");
		directModes = new TraverseModeSet("WALK");
		egressModes = new TraverseModeSet("WALK");
		transitModes = new TraverseModeSet("TRANSIT");
	}
}
