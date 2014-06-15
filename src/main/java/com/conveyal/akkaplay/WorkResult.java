package com.conveyal.akkaplay;

public class WorkResult {

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
