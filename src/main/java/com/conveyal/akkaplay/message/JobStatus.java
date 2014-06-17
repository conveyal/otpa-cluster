package com.conveyal.akkaplay.message;

import java.io.Serializable;

public class JobStatus implements Serializable{

	private static final long serialVersionUID = 1941471771774801942L;
	public int curJobId;
	public float fractionComplete;

	public JobStatus(int curJobId, float fractionComplete) {
		this.curJobId = curJobId;
		this.fractionComplete = fractionComplete;
	}

}
