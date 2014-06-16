package com.conveyal.akkaplay.message;

import java.io.Serializable;

public class PrimeCandidate implements Serializable {
	private static final long serialVersionUID = -6264940355444533421L;
	
	public final long num;
	  public int jobId;
	  
	  public PrimeCandidate(int jobId, long num) {
		  this.jobId = jobId;
		  this.num = num;
	  }
	}
