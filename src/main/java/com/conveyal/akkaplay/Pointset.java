package com.conveyal.akkaplay;

import java.util.List;

public interface Pointset {

	int size();

	Pointset split(int divisions, int i);

	List<Point> getPoints();

}
