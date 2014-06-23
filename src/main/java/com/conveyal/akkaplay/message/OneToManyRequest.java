package com.conveyal.akkaplay.message;

import com.conveyal.akkaplay.Point;
import com.conveyal.akkaplay.Pointset;

public class OneToManyRequest {

	public Point from;
	public Pointset to;

	public OneToManyRequest(Point from, Pointset to) {
		this.from = from;
		this.to = to;
	}

}
