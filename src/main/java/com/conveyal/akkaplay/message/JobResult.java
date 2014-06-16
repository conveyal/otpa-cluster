package com.conveyal.akkaplay.message;

import java.util.ArrayList;

public class JobResult {

	public ArrayList<WorkResult> res;

	public JobResult(ArrayList<WorkResult> res) {
		this.res = res;
	}

}
