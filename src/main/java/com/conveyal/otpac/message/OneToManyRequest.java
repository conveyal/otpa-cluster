package com.conveyal.otpac.message;

import java.util.Date;

import org.opentripplanner.analyst.PointFeature;

public class OneToManyRequest {

	public PointFeature from;
	public Date date;

	public OneToManyRequest(PointFeature from, Date date) {
		this.from = from;
		this.date = date;
	}

}
