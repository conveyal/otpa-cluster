package com.conveyal.akkaplay;

import java.io.Serializable;

public class JobId implements Serializable{

	int jobId;

	public JobId(int jobId) {
		this.jobId = jobId;
	}

	private static final long serialVersionUID = 8909637723423079287L;

}
