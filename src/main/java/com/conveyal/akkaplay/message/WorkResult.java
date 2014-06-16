package com.conveyal.akkaplay.message;

import java.io.Serializable;

public class WorkResult implements Serializable{

	private static final long serialVersionUID = 1701318134569347393L;
	public long num;
	public boolean isPrime;
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