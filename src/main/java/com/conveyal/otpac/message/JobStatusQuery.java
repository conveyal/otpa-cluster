package com.conveyal.otpac.message;

import java.io.Serializable;

/**
 * Inquire after the status of a job.
 */
public class JobStatusQuery implements Serializable{

	private static final long serialVersionUID = 4018400017224422039L;
	
	public int jobId;
	
	public JobStatusQuery(int jobId) {
		this.jobId = jobId;
	}

}
