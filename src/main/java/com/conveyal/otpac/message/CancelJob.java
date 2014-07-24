package com.conveyal.otpac.message;

public class CancelJob {
	public int jobid;

	public CancelJob(int jobid) {
		this.jobid = jobid;
	}
	
	public CancelJob(){
		this.jobid = -1;
	}

}
