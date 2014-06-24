package com.conveyal.akkaplay.message;

import java.util.Date;

import com.conveyal.akkaplay.Point;
import com.conveyal.akkaplay.Pointset;

public class OneToManyRequest {

	public Point from;
	public Date date;

	public OneToManyRequest(Point from, Date date) {
		this.from = from;
		this.date = date;
	}

}
