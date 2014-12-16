package com.conveyal.otpac.message;

import java.io.Serializable;

public class JobResultQuery implements Serializable {
	public int jobId;

	public JobResultQuery(int jobId) {
		this.jobId = jobId;
	}

}
