package com.conveyal.otpac.message;

import java.io.Serializable;

import org.opentripplanner.analyst.SampleSet;
import org.opentripplanner.routing.graph.Graph;
import org.opentripplanner.standalone.Router;

public class SetOneToManyContext implements Serializable {

	public Router router;

	public SetOneToManyContext(Router router) {
		this.router = router;
	}

}
