package com.conveyal.otpac.message;

import org.opentripplanner.analyst.SampleSet;
import org.opentripplanner.standalone.Router;

/**
 * SampleSets are tightly coupled to their graphs, so we pass them around together.
 */
public class GraphAndSampleSet {
	public Router router;
	public SampleSet sampleSet;
	public String pointsetId;
	
	public GraphAndSampleSet(Router router, SampleSet sampleSet,
			String pointsetId) {
		this.router = router;
		this.sampleSet = sampleSet;
		this.pointsetId = pointsetId;
	}
}
