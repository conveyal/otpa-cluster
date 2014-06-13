package com.conveyal.akkaplay;

import java.io.Serializable;

public class FindPrime implements Serializable{

	private static final long serialVersionUID = 185517485358738902L;
	
	long num;

	public FindPrime(long i) {
		this.num=i;
	}

}
