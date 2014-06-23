package com.conveyal.akkaplay.message;

import java.util.Date;

import com.conveyal.akkaplay.Point;
import com.conveyal.akkaplay.Pointset;

public class OneToManyRequest {

	public Point from;
	public Pointset to;
	public Date date;

	public OneToManyRequest(Point from, Pointset to, Date date) {
		this.from = from;
		this.to = to;
		this.date = date;
	}

}
