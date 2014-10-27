package com.conveyal.otpac.message;

import java.io.Serializable;

public class CancelJob implements Serializable{
	private static final long serialVersionUID = 3434520506680469418L;
	public int jobid;

	public CancelJob(int jobid) {
		this.jobid = jobid;
	}
	
	public CancelJob(){
		this.jobid = -1;
	}

}
