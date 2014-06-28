package com.conveyal.akkaplay.message;

import java.util.Date;

import org.opentripplanner.analyst.PointFeature;

import com.conveyal.akkaplay.Point;

public class OneToManyRequest {

	public PointFeature from;
	public Date date;

	public OneToManyRequest(PointFeature from, Date date) {
		this.from = from;
		this.date = date;
	}

}
