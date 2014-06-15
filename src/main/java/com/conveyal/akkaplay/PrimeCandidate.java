package com.conveyal.akkaplay;

import java.io.Serializable;

public class PrimeCandidate implements Serializable {
	  public final long num;
	  public int jobId;
	  
	  public PrimeCandidate(int jobId, long num) {
		  this.jobId = jobId;
		  this.num = num;
	  }
	}
