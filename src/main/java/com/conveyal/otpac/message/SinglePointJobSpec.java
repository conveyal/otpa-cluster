package com.conveyal.otpac.message;

import com.amazonaws.auth.profile.internal.Profile;
import org.opentripplanner.profile.ProfileRequest;
import org.opentripplanner.routing.core.RoutingRequest;

/**
 * Describes a single-point job.
 * Specify an origin in the routing request or profile request.
 */
public class SinglePointJobSpec extends JobSpec {
    /** Should an isochrone be generated? */
    public boolean isochrone;

    /** Should a ResultSet be generated? */
    public boolean resultSet;

    /** is the job done? */
    public boolean complete = false;

    /** request analysis, optionally with an isochrone. specify toPtsLoc=null if you do not want a resultset */
    public SinglePointJobSpec(String graphId, String toPtsLoc, RoutingRequest options, boolean isochrone) {
        super(graphId, null, toPtsLoc, options);
        resultSet = toPtsLoc != null;
        this.isochrone = isochrone;
    }

    /** request analysis without an isochrone */
    public SinglePointJobSpec(String graphId, String toPtsLoc, RoutingRequest options) {
        this(graphId, toPtsLoc, options, false);
    }

    /** request an isochrone */
    public SinglePointJobSpec(String graphId, RoutingRequest options) {
        this(graphId, null, options, true);
    }

    /** request analysis, optionally with an isochrone. specify toPtsLoc=null if you do not want a resultset */
    public SinglePointJobSpec(String graphId, String toPtsLoc, ProfileRequest options, boolean isochrone) {
        super(graphId, null, toPtsLoc, options);
        resultSet = toPtsLoc != null;
        this.isochrone = isochrone;
    }

    /** request analysis without an isochrone */
    public SinglePointJobSpec(String graphId, String toPtsLoc, ProfileRequest options) {
        this(graphId, toPtsLoc, options, false);
    }

    /** request an isochrone */
    public SinglePointJobSpec(String graphId, ProfileRequest options) {
        this(graphId, null, options, true);
    }
}
