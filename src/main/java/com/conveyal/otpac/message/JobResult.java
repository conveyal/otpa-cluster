package com.conveyal.otpac.message;

import java.io.Serializable;
import java.util.ArrayList;

public class JobResult implements Serializable {

	public ArrayList<WorkResult> res;

	public JobResult(ArrayList<WorkResult> res) {
		this.res = res;
	}

}
