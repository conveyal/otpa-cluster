package com.conveyal.otpac.message;

import java.io.Serializable;

public class JobId implements Serializable{

	public int jobId;

	public JobId(int jobId) {
		this.jobId = jobId;
	}

	private static final long serialVersionUID = 8909637723423079287L;

}
