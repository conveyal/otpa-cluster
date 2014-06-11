package com.conveyal.akkaplay;

public class WorkResult {

	long num;
	boolean isPrime;

	public WorkResult(long num, boolean isPrime) {
		this.num = num;
		this.isPrime = isPrime;
	}
	
	public String toString(){
		return num+":"+isPrime;
	}

}
