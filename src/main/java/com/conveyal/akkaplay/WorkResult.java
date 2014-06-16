package com.conveyal.akkaplay;

import java.io.Serializable;

public class WorkResult implements Serializable{

	private static final long serialVersionUID = 1701318134569347393L;
	long num;
	boolean isPrime;
	public int jobId;

	public WorkResult(int jobId, long num, boolean isPrime) {
		this.jobId = jobId;
		this.num = num;
		this.isPrime = isPrime;
	}
	
	public String toString(){
		return num+":"+isPrime;
	}

}
