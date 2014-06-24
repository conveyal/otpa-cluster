package com.conveyal.akkaplay;

import java.util.List;

import org.opentripplanner.common.model.GenericLocation;

public interface Pointset {

	int size();

	Pointset split(int divisions, int i);

	List<Point> getPoints();

	Point get(int i);

}
